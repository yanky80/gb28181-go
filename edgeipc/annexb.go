package edgeipc

func validAccessUnit(codec Codec, payload []byte) bool {
	nals := annexBNALs(payload)
	if len(nals) == 0 {
		return false
	}
	seenVCL := false
	for _, nal := range nals {
		if len(nal) == 0 {
			return false
		}
		if codec == CodecH264 {
			typ := nal[0] & 0x1f
			if nal[0]&0x80 != 0 || typ == 0 || typ > 23 {
				return false
			}
			seenVCL = seenVCL || typ <= 5
			continue
		}
		if len(nal) < 2 || nal[0]&0x80 != 0 || nal[1]&0x07 == 0 {
			return false
		}
		typ := (nal[0] >> 1) & 0x3f
		if typ > 40 {
			return false
		}
		seenVCL = seenVCL || typ <= 31
	}
	return seenVCL
}

func validKeyframe(codec Codec, payload []byte) bool {
	if !validAccessUnit(codec, payload) {
		return false
	}
	seen := make(map[byte]bool, 4)
	for _, nal := range annexBNALs(payload) {
		var typ byte
		if codec == CodecH264 {
			typ = nal[0] & 0x1f
			switch typ {
			case 5, 7, 8:
				seen[typ] = true
			}
			continue
		}
		typ = (nal[0] >> 1) & 0x3f
		switch typ {
		case 19, 20, 32, 33, 34:
			seen[typ] = true
		}
	}
	if codec == CodecH264 {
		return seen[5] && seen[7] && seen[8]
	}
	return (seen[19] || seen[20]) && seen[32] && seen[33] && seen[34]
}

func annexBNALs(payload []byte) [][]byte {
	var nals [][]byte
	for i := 0; i < len(payload); {
		start, prefix := annexBStartCode(payload, i)
		if prefix == 0 {
			break
		}
		body := start + prefix
		end := len(payload)
		for j := body; j < len(payload); j++ {
			if _, nextPrefix := annexBStartCode(payload, j); nextPrefix != 0 {
				end = j
				break
			}
		}
		for end > body && payload[end-1] == 0 {
			end--
		}
		nals = append(nals, payload[body:end])
		i = end
	}
	return nals
}

func annexBStartCode(data []byte, at int) (int, int) {
	if at+3 <= len(data) && data[at] == 0 && data[at+1] == 0 && data[at+2] == 1 {
		return at, 3
	}
	if at+4 <= len(data) && data[at] == 0 && data[at+1] == 0 && data[at+2] == 0 && data[at+3] == 1 {
		return at, 4
	}
	return 0, 0
}
