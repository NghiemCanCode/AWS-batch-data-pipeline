# Mounted at /root/.airbyte (step 5.2): Airbyte's internal database (connections, CDC cursors).
# Kept apart from the instance so replacing the instance does not lose the cursors.
resource "aws_ebs_volume" "data" {
  availability_zone = var.az
  size              = local.data_volume_gb
  type              = "gp3"
  encrypted         = true
  tags              = { Name = "${local.name}-data" }

  # Learning setup: no prevent_destroy, so terraform destroy removes Airbyte's internal database too.
}

resource "aws_volume_attachment" "data" {
  device_name                    = "/dev/sdf"
  volume_id                      = aws_ebs_volume.data.id
  instance_id                    = aws_instance.airbyte.id
  stop_instance_before_detaching = true
}
