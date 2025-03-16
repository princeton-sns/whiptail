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
    p = subprocess.run(cmd, stdout=stdout, stderr=stderr, shell=True, check=True, universal_newlines=True)
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


def main():

    call("apt update")
    call("apt install gnuplot -y")
    call("apt install feh -y")
    call("apt install -y python3-pip")
    call("pip3 install -y numpy")

    setup_vimrc()

    return 0


if __name__ == "__main__":
    sys.exit(main())
