# Each source sends the peering request from its own stack; this stack accepts it and adds the route.
resource "aws_vpc_peering_connection_accepter" "cdc" {
  for_each                  = var.cdc_sources
  vpc_peering_connection_id = each.value.peering_id
  auto_accept               = true
  tags                      = { Name = "${local.name}-from-${each.key}" }
}

resource "aws_route" "to_source" {
  for_each                  = var.cdc_sources
  route_table_id            = aws_route_table.public.id
  destination_cidr_block    = each.value.cidr
  vpc_peering_connection_id = aws_vpc_peering_connection_accepter.cdc[each.key].id
}
