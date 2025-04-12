import subprocess
import sys


def call(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT):
    """
  Calls a command in the shell.

  :param cmd: (str)
  :param stdout: set by default to subprocess.PIPE (which is standard stream)
  :param stderr: set by default subprocess.STDOUT (combines with stdout)
  :return: if successful, stdout stream of command.
  """
    print(cmd)
    p = subprocess.run(cmd, stdout=stdout, stderr=stderr, shell=True,
                       check=True, universal_newlines=True)
    return p.stdout


def call_remote(host, cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT):
    """
  Makes a remote call of a command.
  :param host: (str)
  :param cmd:  (str)
  :param stdout:  set by default to subprocess.PIPE (which is the standard stream)
  :param stderr: set by default to subprocess.STDOUT (combines with stdout)
  :return: if successful, stdout stream of command
  """
    cmd = "sudo ssh {0} '{1}'".format(host, cmd)
    return call(cmd, stdout, stderr)


def setup_vimrc():
    call("echo 'set nu' >> /root/.vimrc")
    call("echo 'set autoindent' >> /root/.vimrc")
    call("echo 'set tabstop=4 shiftwidth=4 expandtab' >> /root/.vimrc")


def setup_aliases():
    call("echo \"alias w=\'cd /root/whiptail\'\" >> /root/.bash_aliases")
    call(
        "echo \"alias e=\'cd /root/whiptail/experiments\'\" >> /root/.bash_aliases")
    call(
        "echo \"alias strongstore=\'cd /root/whiptail/src/store/strongstore\'\" >> /root/.bash_aliases")
    call(
        "echo \"alias r=\'cd /mnt/extra/experiments\'\" >> /root/.bash_aliases")


def setup_perf():
    call("apt install -y linux-tools-4.15.0-151-generic linux-cloud-tools-4.15.0-151-generic linux-tools-generic")

    hosts = [
        "us-east-1-0",
        "us-east-1-1",
        "us-east-1-2",
        "us-west-1-0",
        "us-west-1-1",
        "us-west-1-2",
        "eu-west-1-0",
        "eu-west-1-1",
        "eu-west-1-2",
        "client-0-0",
        "client-0-1",
        "client-1-0",
        "client-1-1",
        "client-2-0",
        "client-2-1"]

    for host in hosts:
        call_remote(host,
                    "apt update && apt install -y linux-tools-4.15.0-151-generic linux-cloud-tools-4.15.0-151-generic linux-tools-generic")


def main():
    call("apt update")
    call("apt install gnuplot -y")
    call("apt install feh -y")
    call("apt install -y python3-pip")
    call("pip3 install numpy")

    setup_vimrc()
    setup_aliases()
    setup_perf()

    return 0


if __name__ == "__main__":
    sys.exit(main())
