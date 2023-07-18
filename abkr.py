'''
    Main file
    Command line interface for a custom database manager
'''

import signal
import sys
import cli


def exit_handler(_1, _2):
    sys.exit(0)


signal.signal(signal.SIGINT, exit_handler)


def main():
    '''
        Main function
        Opens a command line interface to the Database Manager
    '''
    interface = cli.CLI()
    interface.run()


if __name__ == '__main__':
    main()
