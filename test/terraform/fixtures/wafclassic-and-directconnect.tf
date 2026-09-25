resource "aws_waf_byte_match_set" "example" {
  name = "wfdc-byte-match-set"

  byte_match_tuples {
    text_transformation   = "NONE"
    target_string         = "badbot"
    positional_constraint = "CONTAINS"

    field_to_match {
      type = "HEADER"
      data = "User-Agent"
    }
  }
}

resource "aws_waf_geo_match_set" "example" {
  name = "wfdc-geo-match-set"

  geo_match_constraint {
    type  = "Country"
    value = "US"
  }
}

resource "aws_waf_regex_pattern_set" "example" {
  name                  = "wfdc-regex-pattern-set"
  regex_pattern_strings = ["waf-regex-pattern"]
}

resource "aws_waf_regex_match_set" "example" {
  name = "wfdc-regex-match-set"

  regex_match_tuple {
    field_to_match {
      type = "URI"
    }

    regex_pattern_set_id = aws_waf_regex_pattern_set.example.id
    text_transformation  = "NONE"
  }
}

resource "aws_waf_size_constraint_set" "example" {
  name = "wfdc-size-constraint-set"

  size_constraints {
    text_transformation = "NONE"
    comparison_operator = "EQ"
    size                = 4096

    field_to_match {
      type = "BODY"
    }
  }
}

resource "aws_waf_sql_injection_match_set" "example" {
  name = "wfdc-sqli-match-set"

  sql_injection_match_tuples {
    text_transformation = "URL_DECODE"

    field_to_match {
      type = "QUERY_STRING"
    }
  }
}

resource "aws_waf_xss_match_set" "example" {
  name = "wfdc-xss-match-set"

  xss_match_tuples {
    text_transformation = "NONE"

    field_to_match {
      type = "QUERY_STRING"
    }
  }
}

resource "aws_waf_rate_based_rule" "example" {
  name        = "wfdc-rate-rule"
  metric_name = "wfdcRateRule"
  rate_key    = "IP"
  rate_limit  = 2000
}

resource "aws_waf_rule" "example" {
  name        = "wfdc-rule"
  metric_name = "wfdcRule"

  predicates {
    data_id = aws_waf_byte_match_set.example.id
    negated = false
    type    = "ByteMatch"
  }
}

resource "aws_waf_rule_group" "example" {
  name        = "wfdc-rule-group"
  metric_name = "wfdcRuleGroup"

  activated_rule {
    action {
      type = "BLOCK"
    }

    priority = 1
    rule_id  = aws_waf_rule.example.id
  }
}

resource "aws_waf_web_acl" "example" {
  name        = "wfdc-web-acl"
  metric_name = "wfdcWebAcl"

  default_action {
    type = "ALLOW"
  }

  rules {
    action {
      type = "BLOCK"
    }

    priority = 1
    rule_id  = aws_waf_rule.example.id
  }
}

resource "aws_dx_connection" "example" {
  name      = "wfdc-dx-conn"
  bandwidth = "1Gbps"
  location  = "EqDC2"
}

resource "aws_dx_connection" "assoc" {
  name      = "wfdc-dx-conn-assoc"
  bandwidth = "1Gbps"
  location  = "EqDC2"
}

resource "aws_dx_lag" "example" {
  name                  = "wfdc-lag"
  connections_bandwidth = "1Gbps"
  location              = "EqDC2"
}

resource "aws_dx_connection_association" "example" {
  connection_id = aws_dx_connection.assoc.id
  lag_id        = aws_dx_lag.example.id
}

resource "aws_dx_hosted_connection" "example" {
  connection_id    = aws_dx_lag.example.id
  name             = "wfdc-hosted-conn"
  bandwidth        = "500Mbps"
  vlan             = 100
  owner_account_id = "000000000000"
}

resource "aws_dx_connection_confirmation" "example" {
  connection_id = aws_dx_hosted_connection.example.id
}

resource "aws_dx_macsec_key_association" "example" {
  connection_id = aws_dx_connection.example.id
  cak           = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  ckn           = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}

resource "aws_dx_gateway" "example" {
  name            = "wfdc-dxgw"
  amazon_side_asn = 64513
}

resource "aws_dx_private_virtual_interface" "example" {
  connection_id  = aws_dx_connection.example.id
  dx_gateway_id  = aws_dx_gateway.example.id
  name           = "wfdc-private-vif"
  vlan           = 101
  address_family = "ipv4"
  bgp_asn        = 65000
}

resource "aws_dx_public_virtual_interface" "example" {
  connection_id         = aws_dx_connection.example.id
  name                  = "wfdc-public-vif"
  vlan                  = 102
  address_family        = "ipv4"
  bgp_asn               = 65001
  customer_address      = "175.45.176.1/30"
  amazon_address        = "175.45.176.2/30"
  route_filter_prefixes = ["210.52.109.0/24"]
}

resource "aws_dx_transit_virtual_interface" "example" {
  connection_id  = aws_dx_connection.example.id
  dx_gateway_id  = aws_dx_gateway.example.id
  name           = "wfdc-transit-vif"
  vlan           = 103
  address_family = "ipv4"
  bgp_asn        = 65002
}

resource "aws_dx_bgp_peer" "example" {
  virtual_interface_id = aws_dx_private_virtual_interface.example.id
  address_family       = "ipv6"
  bgp_asn              = 65003
}

resource "aws_dx_hosted_private_virtual_interface" "example" {
  connection_id    = aws_dx_connection.example.id
  name             = "wfdc-hosted-private-vif"
  vlan             = 104
  address_family   = "ipv4"
  bgp_asn          = 65004
  owner_account_id = "000000000000"
}

resource "aws_dx_hosted_private_virtual_interface_accepter" "example" {
  virtual_interface_id = aws_dx_hosted_private_virtual_interface.example.id
  dx_gateway_id        = aws_dx_gateway.example.id
}

resource "aws_dx_hosted_public_virtual_interface" "example" {
  connection_id         = aws_dx_connection.example.id
  name                  = "wfdc-hosted-public-vif"
  vlan                  = 105
  address_family        = "ipv4"
  bgp_asn               = 65005
  customer_address      = "175.45.177.1/30"
  amazon_address        = "175.45.177.2/30"
  owner_account_id      = "000000000000"
  route_filter_prefixes = ["210.52.110.0/24"]
}

resource "aws_dx_hosted_public_virtual_interface_accepter" "example" {
  virtual_interface_id = aws_dx_hosted_public_virtual_interface.example.id
}

resource "aws_dx_hosted_transit_virtual_interface" "example" {
  connection_id    = aws_dx_connection.example.id
  name             = "wfdc-hosted-transit-vif"
  vlan             = 106
  address_family   = "ipv4"
  bgp_asn          = 65006
  owner_account_id = "000000000000"
}

resource "aws_dx_hosted_transit_virtual_interface_accepter" "example" {
  virtual_interface_id = aws_dx_hosted_transit_virtual_interface.example.id
  dx_gateway_id        = aws_dx_gateway.example.id
}

resource "aws_ec2_transit_gateway" "proposal" {
  description = "wfdc dx proposal tgw"
}

resource "aws_dx_gateway" "proposal" {
  name            = "wfdc-dxgw-proposal"
  amazon_side_asn = 64514
}

resource "aws_dx_gateway_association_proposal" "example" {
  dx_gateway_id               = aws_dx_gateway.proposal.id
  dx_gateway_owner_account_id = "000000000000"
  associated_gateway_id       = aws_ec2_transit_gateway.proposal.id
}
