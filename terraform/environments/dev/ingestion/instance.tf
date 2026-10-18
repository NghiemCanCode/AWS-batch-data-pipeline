data "aws_ssm_parameter" "ubuntu" {
  name = "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
}

resource "aws_instance" "airbyte" {
  ami                                  = data.aws_ssm_parameter.ubuntu.insecure_value
  instance_type                        = var.instance_type
  subnet_id                            = aws_subnet.public.id
  private_ip                           = local.airbyte_private_ip
  associate_public_ip_address          = true # no NAT: the host reaches the internet with its own IP (changes on every start)
  vpc_security_group_ids               = [aws_security_group.airbyte.id]
  iam_instance_profile                 = aws_iam_instance_profile.airbyte.name
  disable_api_termination              = false # learning setup: terraform destroy can remove it
  instance_initiated_shutdown_behavior = "stop"
  user_data = templatefile("${path.module}/templates/cloud-init.yaml.tftpl", {
    auto_stop_after = local.auto_stop_after
    disable_ssh     = true
  })

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
    # Connector pods run in kind, inside Docker: pod -> kind node -> host is 2 extra network hops.
    # With the default hop limit 1, IMDSv2 tokens never reach the pod and the S3 destination finds no credentials.
    http_put_response_hop_limit = 3
  }

  root_block_device {
    volume_size           = local.root_volume_gb
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }

  credit_specification {
    cpu_credits = "unlimited" # a CDC initial snapshot pins the CPU for a long time
  }

  tags = { Name = local.name }

  lifecycle { ignore_changes = [ami, user_data] }
}
