# Copyright 2017, Nan Dun <nan.dun@acm.org>
# All rights reserved.

# Terraform Configurations

variable "aws" {
    type = "map"
    default = {
        region          = ""
        ami             = ""
        key_name        = ""
        route53_zone    = ""
        security_group  = ""
        subnet_id       = ""
        monitoring      = ""
        vpc_id          = ""
        associate_public_ip_address = ""
  		iam_instance_profile = ""
        in_ssh_cidr_block = ""
    }
}

variable "terraform" {
    type = "map"
    default = {
        backend = ""
        region  = ""
        bucket  = ""
    }
}

variable "tags" {
    type = "map"
    default = {
        environment = ""
        user        = ""
    }
}

variable "webserver" {
    type = "map"
    default = {
        instance_type        = ""
        count                = ""
        root_volume_type     = ""
        root_volume_size     = ""
        root_volume_delete   = ""
        in_http_cidr_block   = ""
    }
}

variable "mapper" {
    type = "map"
    default = {
        instance_type   = ""
        count           = 0

        ebs_device_name = ""
        ebs_volume_type = ""
        ebs_volume_size = ""
        ebs_volume_deletion = ""
    }
}

variable "reducer" {
    type = "map"
    default = {
        instance_type = ""
        count         = 0
    }
}

provider "aws" {
    region = "${var.aws["region"]}"
}

### EC2 Resources ###

# Web Server
resource "aws_instance" "webserver" {
    ami                         = "${var.aws["ami"]}"
    vpc_security_group_ids      = [ "${aws_security_group.default.id}", "${aws_security_group.webserver.id}" ]
    subnet_id                   = "${var.aws["subnet_id"]}"
    key_name                    = "${var.aws["key_name"]}"
    monitoring                  = "${var.aws["monitoring"]}"
    associate_public_ip_address = "${var.aws["associate_public_ip_address"]}"
    iam_instance_profile        = "${var.aws["iam_instance_profile"]}"

    instance_type               = "${var.webserver["instance_type"]}"
    count                       = "${var.webserver["count"]}"

    root_block_device {
        volume_type = "${var.webserver["root_volume_type"]}"
        volume_size = "${var.webserver["root_volume_size"]}"
        delete_on_termination = "${var.webserver["root_volume_delete"]}"
    }

    tags {
        Environment = "${var.tags["environment"]}"
        User        = "${var.tags["user"]}"
        Group       = "webserver"
        Name        = "webserver${count.index}"
    }
}

# Mapper
resource "aws_instance" "mapper" {
    ami                         = "${var.aws["ami"]}"
    vpc_security_group_ids      = [ "${aws_security_group.default.id}", "${aws_security_group.mapper.id}" ]
    subnet_id                   = "${var.aws["subnet_id"]}"
    key_name                    = "${var.aws["key_name"]}"
    monitoring                  = "${var.aws["monitoring"]}"
    associate_public_ip_address = "${var.aws["associate_public_ip_address"]}"
    iam_instance_profile        = "${var.aws["iam_instance_profile"]}"

    instance_type               = "${var.mapper["instance_type"]}"
    count                       = "${var.mapper["count"]}"

    ebs_block_device {
        device_name = "${var.mapper["ebs_device_name"]}"
        volume_size = "${var.mapper["ebs_volume_size"]}"
        volume_type = "${var.mapper["ebs_volume_type"]}"
        delete_on_termination = "${var.mapper["ebs_deletion"]}"
    }

    tags {
        Environment = "${var.tags["environment"]}"
        User        = "${var.tags["user"]}"
        Group       = "mapper"
        Name        = "mapper${count.index}"
    }
}

# Reducer
resource "aws_instance" "reducer" {
    ami                         = "${var.aws["ami"]}"
    vpc_security_group_ids      = [ "${aws_security_group.default.id}", "${aws_security_group.reducer.id}" ]
    subnet_id                   = "${var.aws["subnet_id"]}"
    key_name                    = "${var.aws["key_name"]}"
    monitoring                  = "${var.aws["monitoring"]}"
    associate_public_ip_address = "${var.aws["associate_public_ip_address"]}"
    iam_instance_profile        = "${var.aws["iam_instance_profile"]}"

    instance_type               = "${var.reducer["instance_type"]}"
    count                       = "${var.reducer["count"]}"

    tags {
        Environment = "${var.tags["environment"]}"
        User        = "${var.tags["user"]}"
        Group       = "reducer"
        Name        = "reducer${count.index}"
    }
}

### Security Groups ###
resource "aws_security_group" "default" {
    vpc_id = "${var.aws["vpc_id"]}"
    name = "default-security-group-${var.tags["environment"]}"
    description = "default security group in ${var.tags["environment"]}"

    # Allow all traffic within the default group
    ingress {
        from_port = 0
        to_port = 0
        protocol = "-1"
        self = "true"
    }
    egress {
        from_port = 0
        to_port = 0
        protocol = "-1"
        self = "true"
    }

    # Allow inbound SSH
    ingress {
        from_port = 22
        to_port = 22
        protocol = "tcp"
        cidr_blocks = ["${var.aws["in_ssh_cidr_block"]}"]
    }

    # Allow all outbound
    egress {
        from_port = 0
        to_port = 0
        protocol = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }

    tags {
        Environment = "${var.tags["environment"]}"
        Name        = "default-security-group-${var.tags["environment"]}"
    }
}

resource "aws_security_group" "webserver" {
    vpc_id = "${var.aws["vpc_id"]}"
    name = "webserver-security-group-${var.tags["environment"]}"
    description = "webserver security group in ${var.tags["environment"]}"

    // allow inbound HTTP
    ingress {
        from_port = 80
        to_port = 80
        protocol = "tcp"
        cidr_blocks = [ "${var.webserver["in_http_cidr_block"]}" ]
    }

    ingress {
        from_port = 5006
        to_port = 5006
        protocol = "tcp"
        cidr_blocks = [ "${var.webserver["in_http_cidr_block"]}" ]
    }

    tags {
        Environment = "${var.tags["environment"]}"
        User        = "${var.tags["user"]}"
        Name = "webserver-security-group-${var.tags["environment"]}"
    }
}

resource "aws_security_group" "mapper" {
    vpc_id = "${var.aws["vpc_id"]}"
    name = "mapper-security-group-${var.tags["environment"]}"
    description = "mapper security group in ${var.tags["environment"]}"

    // allow outbound to SQS
    ingress {
        from_port = 443
        to_port = 443
        protocol = "tcp"
        cidr_blocks = ["${var.mapper["out_sqs_cidr_block"]}"]
    }

    tags {
        Environment = "${var.tags["environment"]}"
        User        = "${var.tags["user"]}"
        Name        = "mapper-security-group-${var.tags["environment"]}"
    }
}

resource "aws_security_group" "reducer" {
    vpc_id = "${var.aws["vpc_id"]}"
    name = "reducer-security-group-${var.tags["environment"]}"
    description = "reducer security group in ${var.tags["environment"]}"

    // allow outbound to S3
    tags {
        Environment = "${var.tags["environment"]}"
        User        = "${var.tags["user"]}"
        Name        = "reducer-security-group-${var.tags["environment"]}"
    }
}

### Route 53 Records ###
resource "aws_route53_record" "webserver" {
    zone_id = "${var.aws["route53_zone"]}"
    count   = "${var.webserver["count"]}"
    name    = "${element(aws_instance.webserver.*.tags.Name, count.index)}"
    type    = "A"
    ttl     = "300"
    records = ["${element(aws_instance.webserver.*.public_ip, count.index)}"]
}

resource "aws_route53_record" "web" {
    zone_id = "${var.aws["route53_zone"]}"
    name    = "web"
    type    = "CNAME"
    ttl     = "300"
    records = ["${element(aws_route53_record.webserver.*.fqdn, 0)}"]
}

resource "aws_route53_record" "mapper" {
    zone_id = "${var.aws["route53_zone"]}"
    count   = "${var.mapper["count"]}"
    name    = "${element(aws_instance.mapper.*.tags.Name, count.index)}"
    type    = "A"
    ttl     = "300"
    records = ["${element(aws_instance.mapper.*.public_ip, count.index)}"]
}

resource "aws_route53_record" "reducer" {
    zone_id = "${var.aws["route53_zone"]}"
    count   = "${var.reducer["count"]}"
    name    = "${element(aws_instance.reducer.*.tags.Name, count.index)}"
    type    = "A"
    ttl     = "300"
    records = ["${element(aws_instance.reducer.*.public_ip, count.index)}"]
}

output "webserver"  {
    value = ["${aws_route53_record.webserver.*.fqdn}"]
}

output "mapper" {
    value = ["${aws_route53_record.mapper.*.fqdn}"]
}

output "reducer" {
    value = ["${aws_route53_record.reducer.*.fqdn}"]
}
