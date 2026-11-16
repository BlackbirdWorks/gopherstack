package gcpidentitytoolkit

import "strings"

// callingCodes maps ITU country calling codes to the primary ISO 3166-1 alpha-2 region.
const callingCodes = "7:RU 20:EG 27:ZA 30:GR 31:NL 32:BE 33:FR 34:ES 36:HU 39:IT 40:RO 41:CH 43:AT 44:GB 45:DK 46:SE " +
	"47:NO 48:PL 49:DE 51:PE 52:MX 53:CU 54:AR 55:BR 56:CL 57:CO 58:VE 60:MY 61:AU 62:ID 63:PH 64:NZ 65:SG 66:TH " +
	"81:JP 82:KR 84:VN 86:CN 90:TR 91:IN 92:PK 93:AF 94:LK 95:MM 98:IR 211:SS 212:MA 213:DZ 216:TN 218:LY 220:GM " +
	"221:SN 222:MR 223:ML 224:GN 225:CI 226:BF 227:NE 228:TG 229:BJ 230:MU 231:LR 232:SL 233:GH 234:NG 235:TD " +
	"236:CF 237:CM 238:CV 239:ST 240:GQ 241:GA 242:CG 243:CD 244:AO 245:GW 248:SC 249:SD 250:RW 251:ET 252:SO " +
	"253:DJ 254:KE 255:TZ 256:UG 257:BI 258:MZ 260:ZM 261:MG 263:ZW 264:NA 265:MW 266:LS 267:BW 268:SZ 269:KM " +
	"291:ER 297:AW 298:FO 299:GL 350:GI 351:PT 352:LU 353:IE 354:IS 355:AL 356:MT 357:CY 358:FI 359:BG 370:LT " +
	"371:LV 372:EE 373:MD 374:AM 375:BY 376:AD 377:MC 378:SM 380:UA 381:RS 382:ME 383:XK 385:HR 386:SI 387:BA " +
	"389:MK 420:CZ 421:SK 423:LI 500:FK 501:BZ 502:GT 503:SV 504:HN 505:NI 506:CR 507:PA 509:HT 591:BO 592:GY " +
	"593:EC 595:PY 597:SR 598:UY 673:BN 674:NR 675:PG 676:TO 677:SB 678:VU 679:FJ 685:WS 686:KI 852:HK 853:MO " +
	"855:KH 856:LA 880:BD 886:TW 960:MV 961:LB 962:JO 963:SY 964:IQ 965:KW 966:SA 967:YE 968:OM 971:AE 972:IL " +
	"973:BH 974:QA 975:BT 976:MN 977:NP 992:TJ 993:TM 994:AZ 995:GE 996:KG 998:UZ"

const nanpCanada = " 204 226 236 249 250 263 289 306 343 354 365 367 368 382 403 416 418 431 437 438 450 468 474 " +
	"506 514 519 548 579 581 584 587 604 613 639 647 672 683 705 709 742 753 778 780 807 819 825 867 873 902 905 942 "

const nanpOther = "242:BS 246:BB 264:AI 268:AG 284:VG 340:VI 345:KY 441:BM 473:GD 649:TC 664:MS 670:MP 671:GU 684:AS " +
	"721:SX 758:LC 767:DM 784:VC 787:PR 939:PR 809:DO 829:DO 849:DO 868:TT 869:KN 876:JM"

func lookupPair(table, key string) string {
	for f := range strings.FieldsSeq(table) {
		if k, v, ok := strings.Cut(f, ":"); ok && k == key {
			return v
		}
	}

	return ""
}

const (
	nanpLen     = 4
	maxCodeLen  = 3
	nanpAreaLen = 3
)

// phoneRegion returns the ISO region of an E.164 number, or "" when the calling code is unknown.
func phoneRegion(e164num string) string {
	digits := strings.TrimPrefix(e164num, "+")

	if strings.HasPrefix(digits, "1") && len(digits) >= nanpLen {
		area := digits[1:nanpLen]
		if strings.Contains(nanpCanada, " "+area+" ") {
			return "CA"
		}

		if r := lookupPair(nanpOther, area); r != "" {
			return r
		}

		return "US"
	}

	for n := 1; n <= maxCodeLen && n <= len(digits); n++ {
		if r := lookupPair(callingCodes, digits[:n]); r != "" {
			return r
		}
	}

	return ""
}
