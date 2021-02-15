data "http" "myip" {
  url = "https://icanhazip.com"
}

resource "aws_security_group" "ssh" {
  name   = "flintrock-bastion-ssh"
  vpc_id = aws_vpc.main.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["${chomp(data.http.myip.body)}/32"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "bastion" {
  ami                         = "ami-0a887e401f7654935"
  instance_type               = "t2.nano"
  key_name                    = var.ec2_key_name
  subnet_id                   = aws_subnet.public.id
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.ssh.id]

  tags = {
    Name = "flintrock-bastion"
  }

  connection {
    host        = self.public_ip
    user        = "ec2-user"
    private_key = file(var.ssh_key_path)
  }

  provisioner "remote-exec" {
    inline = [
      "mkdir -p /home/ec2-user/.aws/",
    ]
  }

  provisioner "file" {
    source      = var.aws_credentials_path
    destination = "/home/ec2-user/.aws/credentials"
  }

  provisioner "file" {
    source      = var.ssh_key_path
    destination = "/home/ec2-user/.ssh/${var.ec2_key_name}.pem"
  }

  provisioner "remote-exec" {
    inline = [
      "chmod go-rwx /home/ec2-user/.ssh/${var.ec2_key_name}.pem",
    ]
  }

  provisioner "remote-exec" {
    inline = [
      "mkdir -p /home/ec2-user/.config/flintrock/",
    ]
  }

  provisioner "file" {
    source      = var.flintrock_config_path
    destination = "/home/ec2-user/.config/flintrock/config.yaml"
  }

  provisioner "remote-exec" {
    inline = [
      "sudo yum install -y python3",
      "python3 -m venv /home/ec2-user/venv",
      "/home/ec2-user/venv/bin/pip install PyYAML",
    ]
  }

  provisioner "remote-exec" {
    inline = [
      <<-EOM
        /home/ec2-user/venv/bin/python << EO_PYTHON
        import yaml
        with open('/home/ec2-user/.config/flintrock/config.yaml') as f:
            config = yaml.safe_load(f)
        config['providers']['ec2']['key-name'] = '${var.ec2_key_name}'
        config['providers']['ec2']['identity-file'] = '/home/ec2-user/.ssh/${var.ec2_key_name}.pem'
        config['providers']['ec2']['vpc-id'] = '${aws_vpc.main.id}'
        config['providers']['ec2']['subnet-id'] = '${aws_subnet.private.id}'
        config['providers']['ec2']['authorize-access-from'] = ['${self.private_ip}']
        with open('/home/ec2-user/.config/flintrock/config.yaml', 'w') as f:
            yaml.dump(config, f, indent=2)
        EO_PYTHON
        EOM
    ]
  }
}

output "bastion_ip" {
  value = aws_instance.bastion.public_ip
}
