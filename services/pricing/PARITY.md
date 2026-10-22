---
service: pricing
sdk_module: aws-sdk-go-v2/service/pricing@v1.49.4
last_audit_commit: 4ae40e79e
last_audit_date: 2026-10-18
overall: B            # all 5 SDK operations serve a real embedded catalog through the real SDK client; B because the catalog is small and illustrative.
ops:
  DescribeServices: {wire: ok, errors: ok, state: ok, persist: ok, note: "five catalog services; AttributeNames derived from the products; FormatVersion aws_v1 only"}
  GetAttributeValues: {wire: ok, errors: ok, state: ok, persist: ok, note: "distinct sorted values; unknown attribute is NotFoundException"}
  GetPriceListFileUrl: {wire: ok, errors: ok, state: ok, persist: ok, note: "URL served by gopherstack at /_gopherstack/pricing/offers/v1.0/aws/<service>/<version>/<region>/index.<json|csv>"}
  GetProducts: {wire: ok, errors: ok, state: ok, persist: ok, note: "TERM_MATCH/EQUALS exact, CONTAINS substring, ANY_OF/NONE_OF comma lists; all filters ANDed; each PriceList element is the real product+terms JSON string"}
  ListPriceLists: {wire: ok, errors: ok, state: ok, persist: ok, note: "one list per region with csv and json formats; EffectiveDate before the publication date yields none; ResourceNotFoundException for unknown service"}
families:
  Catalog: {status: partial, note: "AmazonEC2 (instances Linux/Windows with 1y/3y No Upfront Reserved terms, EBS volumes), AmazonS3 (storage classes with tiers, request pricing), AWSLambda, AmazonDynamoDB, AmazonRDS (MySQL/PostgreSQL, Single/Multi-AZ, Reserved terms) across six regions. Prices are an illustrative us-east-1 snapshot scaled per region, not live AWS rates."}
  Files: {status: ok, note: "JSON file uses the published envelope (formatVersion, offerCode, version, products, terms); CSV has the five header rows and every field quoted."}
gaps: []
items_still_open:
  - Catalog: ten services (EC2, S3, Lambda, DynamoDB, RDS, SQS, SNS, CloudFront, ElastiCache, ECS/Fargate) and 14 regions; DescribeServices does not list the other ~200 AWS service codes
  - Catalog: prices are illustrative, not the live AWS price feed; there is a single published version (20261001000000)
  - Reserved: EC2/RDS carry the 12 standard/convertible x 1yr/3yr x No/Partial/All Upfront terms, but ratios are illustrative and the non-Standard-No-Upfront offer term codes are recalled from the public price list, unverified
  - Savings Plans: only ComputeSavingsPlans price lists (JSON only, EC2 Linux rates) are served through ListPriceLists/GetPriceListFileUrl; the file layout is recalled from the published format and unverified; other SP types, GetProducts and DescribeServices do not expose them
  - AccessDeniedException, ThrottlingException, InternalErrorException, ExpiredNextTokenException: never raised
structural_gaps: []
deferred: []
leaks: {status: clean, note: "stateless; the catalog is built once with sync.OnceValue"}
---

## Notes
Protocol awsjson1.1, X-Amz-Target prefix AWSPriceListService; homed in us-east-1 (AWS also serves ap-south-1 and eu-central-1 endpoints).
The service holds no mutable state, so it has no persistence.go; Reset is a no-op.
The file route is registered by internal/app/budgets_wiring.go (registerPricingEndpoints) outside the service router.
