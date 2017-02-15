# AWS Settings
aws = {
  region         = "us-west-2"
  key_name       = "dun"
  ami            = "ami-8654d1e6"
  subnet_id      = "subnet-dd26e594"
  subnet_ids     = "subnet-dd26e594,subnet-7fa51618"
  route53_zone   = "Z2GWJPXVAX0OOB"
  monitoring     = "false"
  vpc_id         = "vpc-51a73336"
  associate_public_ip_address = "true"
  in_ssh_cidr_block = "0.0.0.0/0"
  iam_instance_profile = "Taxi-EC2-Instance-Profile"
}

# Terraform Settings
terraform = {
  backend = "s3"
  region  = "us-west-2"
  bucket  = "bds-cam"
}

# Tags
tags = {
  environment = "demo"
  user        = "dun"
}

# Web Server Settings
webserver = {
  instance_type        = "t2.micro"
  count                = "2"
  root_volume_type     = "gp2"
  root_volume_size     = "8"
  in_http_cidr_block   = "0.0.0.0/0"
}

# Mapper Settings
mapper = {
  instance_type        = "t2.micro"
  count                = "0"
  ebs_device_name      = "/dev/sdb"
  ebs_volume_size      = 8
  ebs_volume_type      = "gp2"
  ebs_volume_deletion  = "true"
  out_sqs_cidr_block   = "0.0.0.0/0"
}

# Reducer Settings
reducer = {
  instance_type        = "t2.micro"
  count                = "0"
}
