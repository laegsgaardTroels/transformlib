from subprocess import Popen, PIPE


def test_main():
    process = Popen(['transform', 'tests/transforms/squares/*', '-v'], stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
