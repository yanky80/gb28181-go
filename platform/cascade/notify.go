package cascade

// Catalog-change NOTIFY (#370): answer the upper platform's SUBSCRIBE and
// push Catalog NOTIFYs when the local camera set changes, so uppers no longer
// depend on their polling fallback.

import (
	"encoding/xml"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ghettovoice/gosip/sip"
	"github.com/mickeyzzc/gb28181-go/manscdp"
)

// catalogSub is one active catalog subscription (an upper's SUBSCRIBE dialog).
type catalogSub struct {
	upper    *upper
	callID   string
	fromUser string
	toUser   string
	expires  time.Time
	sendMu   sync.Mutex
}

// notifyScanInterval is how often the camera set is diffed for changes.
var notifyScanInterval = 10 * time.Second

// onSubscribe answers an upper platform's catalog SUBSCRIBE and records the
// dialog for change-driven NOTIFYs. Non-catalog events get Expires 0 (upper
// falls back to polling).
func (s *Service) onSubscribe(req sip.Request, _ sip.ServerTransaction) {
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	if s.stopping.Load() || s.ctx == nil || s.ctx.Err() != nil {
		_, _ = s.srv.RespondOnRequest(req, 503, "Service Unavailable", "", nil)
		return
	}
	event := ""
	for _, h := range req.GetHeaders("Event") {
		if e, ok := h.(*sip.Event); ok {
			event = strings.TrimSpace(e.Value())
			break
		}
		if g, ok := h.(*sip.GenericHeader); ok {
			event = strings.TrimSpace(g.Contents)
			break
		}
	}
	expires := 3600
	for _, h := range req.GetHeaders("Expires") {
		switch hv := h.(type) {
		case *sip.Expires:
			if *hv > 0 {
				expires = int(*hv)
			}
		case *sip.GenericHeader:
			if v := strings.TrimSpace(hv.Contents); v != "" {
				if n, err := strconv.Atoi(v); err == nil && n > 0 {
					expires = n
				}
			}
		}
		break
	}
	if !strings.Contains(strings.ToLower(event), "catalog") {
		zero := sip.Expires(0)
		_, _ = s.srv.RespondOnRequest(req, 200, "OK", "", []sip.Header{&zero})
		return
	}

	u := s.upperOf(req)
	if u == nil {
		_, _ = s.srv.RespondOnRequest(req, 403, "Forbidden", "", nil)
		return
	}
	callID := ""
	if h, ok := req.CallID(); ok {
		callID = h.String()
	}
	fromUser, toUser := reqIDs(req)
	expHdr := sip.Expires(uint32(expires))
	_, _ = s.srv.RespondOnRequest(req, 200, "OK", "", []sip.Header{&expHdr})

	sub := &catalogSub{
		upper:    u,
		callID:   callID,
		fromUser: fromUser,
		toUser:   toUser,
		expires:  time.Now().Add(time.Duration(expires) * time.Second),
	}
	s.mu.Lock()
	s.subs[callID] = sub
	s.mu.Unlock()
	slog.Info("gb28181-cascade: catalog subscription active",
		"upper", u.cfg.ServerAddr, "expires", expires)
	// A fresh subscription immediately gets the current catalog state.
	go s.sendCatalogNotify(sub)
}

// catalogNotifyLoop diffs the camera set and pushes NOTIFYs on change until
// the service stops.
func (s *Service) catalogNotifyLoop() {
	defer s.wg.Done()
	last := s.cameraFingerprint()
	for {
		if !sleepCtx(s.ctx, notifyScanInterval) {
			return
		}
		cur := s.cameraFingerprint()
		if cur == last {
			continue
		}
		last = cur
		s.mu.Lock()
		subs := make([]*catalogSub, 0, len(s.subs))
		expired := make([]*catalogSub, 0)
		now := time.Now()
		for k, sub := range s.subs {
			if now.After(sub.expires) {
				delete(s.subs, k)
				expired = append(expired, sub)
				continue
			}
			subs = append(subs, sub)
		}
		s.mu.Unlock()
		for _, sub := range expired {
			sub.sendMu.Lock()
			sub.sendMu.Unlock()
		}
		for _, sub := range subs {
			go s.sendCatalogNotify(sub)
		}
	}
}

// cameraFingerprint summarizes the camera set for change detection.
func (s *Service) cameraFingerprint() string {
	cams := s.src.Cameras()
	parts := make([]string, 0, len(cams))
	for _, c := range cams {
		parts = append(parts, strings.Join([]string{
			c.ID,
			c.Name,
			c.Brand,
			c.Model,
			s.cameraStatus(c.ID),
			strconv.FormatBool(c.CascadeHidden),
		}, "\x00"))
	}
	sort.Strings(parts)
	return strings.Join(parts, "\x00")
}

// catalogNotifyBody mirrors manscdp.Catalog under a Notify root — the
// subscription-push form of the catalog (GB/T 28181-2016 § 9.5.4).
type catalogNotifyBody struct {
	XMLName  xml.Name       `xml:"Notify"`
	CmdType  string         `xml:"CmdType"`
	SN       int            `xml:"SN"`
	DeviceID string         `xml:"DeviceID"`
	SumNum   int            `xml:"SumNum"`
	Items    []manscdp.Item `xml:"DeviceList>Item"`
}

// sendCatalogNotify pushes the full catalog to one subscription (full-list
// form — receivers merge; deltas are optional in the standard).
func (s *Service) sendCatalogNotify(sub *catalogSub) {
	if s.srv == nil || sub == nil {
		return
	}
	sub.sendMu.Lock()
	defer sub.sendMu.Unlock()
	s.mu.Lock()
	active := s.subs[sub.callID] == sub
	s.mu.Unlock()
	if !active {
		return
	}
	items, err := s.catalogItems()
	if err != nil {
		slog.Warn("gb28181-cascade: catalog build for NOTIFY failed", "error", err)
		return
	}
	body, err := xml.Marshal(catalogNotifyBody{
		CmdType:  "Catalog",
		SN:       int(s.sn.Add(1)),
		DeviceID: sub.upper.cfg.LocalDeviceID,
		SumNum:   len(items),
		Items:    items,
	})
	if err != nil {
		return
	}
	full := append([]byte(xml.Header), body...)

	dst, err := upperAddr(sub.upper)
	if err != nil {
		return
	}
	host, port := s.localHostPort(sub.upper)
	p := sip.Port(port)
	dstPort := sip.Port(dst.Port)
	rb := sip.NewRequestBuilder()
	rb.SetMethod(sip.NOTIFY)
	rb.SetFrom(&sip.Address{Uri: &sip.SipUri{FUser: sip.String{Str: sub.toUser}, FHost: host, FPort: &p}})
	rb.SetTo(&sip.Address{Uri: &sip.SipUri{FUser: sip.String{Str: sub.fromUser}, FHost: dst.IP.String()}})
	rb.SetRecipient(&sip.SipUri{FUser: sip.String{Str: sub.fromUser}, FHost: dst.IP.String(), FPort: &dstPort})
	cid := sip.CallID(bareCallID(sub.callID))
	rb.SetCallID(&cid)
	rb.SetHost(host)
	rb.SetSeqNo(2)
	rb.AddVia(&sip.ViaHop{
		Host: host,
		Port: &p,
		Params: sip.NewParams().
			Add("branch", sip.String{Str: sip.GenerateBranch()}).
			Add("rport", sip.String{}),
	})
	ct := sip.ContentType("Application/MANSCDP+xml")
	rb.SetContentType(&ct)
	rb.SetBody(string(full))
	req, err := rb.Build()
	if err != nil {
		slog.Warn("gb28181-cascade: NOTIFY build failed", "error", err)
		return
	}
	req.AppendHeader(&sip.GenericHeader{HeaderName: "Event", Contents: "Catalog"})
	req.AppendHeader(&sip.GenericHeader{HeaderName: "Subscription-State", Contents: "active;expires=3600"})
	if _, err := s.srv.Request(req); err != nil {
		slog.Warn("gb28181-cascade: catalog NOTIFY failed", "upper", sub.upper.cfg.ServerAddr, "error", err)
		return
	}
	slog.Info("gb28181-cascade: catalog NOTIFY sent",
		"upper", sub.upper.cfg.ServerAddr, "channels", len(items))
}
