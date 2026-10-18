# Placeholder only. The real value is set in step 4.2; Terraform never sees it.
resource "aws_ssm_parameter" "admin_password" {
  name  = "${local.param_prefix}/admin-password"
  type  = "SecureString"
  value = "set-by-script"
  lifecycle { ignore_changes = [value] }
}
