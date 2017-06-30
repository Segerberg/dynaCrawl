#!/usr/bin/env python
import sys
import logging
import os
import socket
import threading
import time
import signal
import traceback
import warnings
import argparse
import subprocess
import tempfile
import shutil
try:
    import warcprox
    import warcprox.main
    import requests
    from chromote import Chromote

except ImportError as e:
    logging.critical(
            '%s: %s\n\nYou might need to run "pip install -r requirements.txt".\nSee README.rst for more information.',
            type(e).__name__, e)
    sys.exit(1)


__version__ = '0.0.2'

def _build_arg_parser(argv=None):
    argv = argv or sys.argv
    arg_parser = argparse.ArgumentParser(
            prog=os.path.basename(argv[0]), description=(

                'Browser based crawling with chrome and warcprox'))

    # common args

    arg_parser.add_argument(
            '-d', '--warcs-dir', dest='warcs_dir', default='./warcs',
            help='where to write warcs')

    # warcprox args
    arg_parser.add_argument(
            '-c', '--cacert', dest='cacert',
            default='./%s-warcprox-ca.pem' % socket.gethostname(),
            help=(
                'warcprox CA certificate file; if file does not exist, it '
                'will be created'))
    arg_parser.add_argument(
            '--certs-dir', dest='certs_dir',
            default='./%s-warcprox-ca' % socket.gethostname(),
            help='where warcprox will store and load generated certificates')
    arg_parser.add_argument(
            '--onion-tor-socks-proxy', dest='onion_tor_socks_proxy',
            default=None, help=(
                'host:port of tor socks proxy, used only to connect to '
                '.onion sites'))

    arg_parser.add_argument(
        '-q', '--quiet', dest='log_level', action='store_const',
        default=logging.INFO, const=logging.WARN, help=(
            'quiet logging, only warnings and errors'))
    arg_parser.add_argument(
        '-v', '--verbose', dest='log_level', action='store_const',
        default=logging.INFO, const=logging.DEBUG, help=(
            'verbose logging'))
    arg_parser.add_argument(
        '--trace', dest='log_level', action='store_const',
        default=logging.INFO, const=5, help=(
            'very verbose logging'))

    arg_parser.add_argument(
        '--version', action='version',
        version='dynacrawl %s - %s' % (
            __version__, os.path.basename(argv[0])))

    return arg_parser



def configure_logging(args):
    logging.basicConfig(
        stream=sys.stderr, level=args.log_level, format=(
            '%(asctime)s %(process)d %(levelname)s %(threadName)s '
            '%(name)s.%(funcName)s(%(filename)s:%(lineno)d) %(message)s'))
    logging.getLogger('requests.packages.urllib3').setLevel(logging.WARN)
    logging.getLogger().addHandler(logging.FileHandler('testlogny'))
    warnings.simplefilter(
        'ignore', category=requests.packages.urllib3.exceptions.InsecureRequestWarning)
    warnings.simplefilter(
        'ignore', category=requests.packages.urllib3.exceptions.InsecurePlatformWarning)



class mainController:
    logger = logging.getLogger(__module__ + "." + __qualname__)

    def __init__(self, args):
        self.stop = threading.Event()
        self.args = args
        self.warcprox_controller = warcprox.main.init_controller(
                self._warcprox_args(args))


    def suggest_default_chrome_exe(self):
        # mac os x application executable paths
        for path in [
            '/Applications/Chromium.app/Contents/MacOS/Chromium',
            '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome']:
            if os.path.exists(path):
                return path
        for exe in [
            'chromium-browser', 'chromium', 'google-chrome',
            'google-chrome-stable', 'google-chrome-beta',
            'google-chrome-unstable']:
            if shutil.which(exe):
                return exe
        return 'chromium-browser'

    def browser(self):
        sock = socket.socket()
        sock.bind(('0.0.0.0', 0))
        port = sock.getsockname()[1]-1
        sock.close()
        self._home_tmpdir = tempfile.TemporaryDirectory()
        self.new_env = os.environ.copy()
        self.new_env['HOME'] = self._home_tmpdir.name
        self._chrome_user_data_dir = os.path.join(
            self._home_tmpdir.name, 'chrome-user-data')
        self.chrome_exe = self.suggest_default_chrome_exe()
        self.chrome_args = [
            self.chrome_exe,
            '--remote-debugging-port=%s' % 9222,
            '--use-mock-keychain',  # mac thing
            '--user-data-dir=%s' % self._chrome_user_data_dir,
            '--disable-web-sockets', '--disable-cache',
            '--window-size=1100,900', '--no-default-browser-check',
            '--disable-first-run-ui', '--no-first-run',
            '--homepage=about:blank', '--disable-direct-npapi-requests',
            '--disable-web-security', '--disable-notifications',
            '--disable-extensions', '--disable-save-password-bubble',
            '--proxy-server=127.0.0.1:%s' % port,'--ignore-certificate-errors']


        self.chrome_process = subprocess.Popen(
            self.chrome_args, env=self.new_env, start_new_session=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)



    def loadSeeds(self, seedFile):
        self.numberOfSeeds = 0
        self.seedList = []
        self.seedFile = seedFile
        with open(self.seedFile) as sf:
            for line in sf:
                self.numberOfSeeds = self.numberOfSeeds + 1
                self.seedList.append(line)
        logging.info('Loaded {} seeds from file'.format(self.numberOfSeeds))
        return self.seedList

    def runJob(self):

        self.chromeBrowser = Chromote()
        self.tab = self.chromeBrowser.tabs[0]
        self.loadSeeds('seeds.txt')
        logging.info('Starting harvest')
        seedCount = 0
        for seed in self.seedList:
            seedCount = seedCount + 1
            print('{} of {}'.format(seedCount, self.numberOfSeeds))
            logging.info('{}'.format(seed))
            self.tab.set_url(seed)
            time.sleep(10)
        self.shutdown()







    def start(self):
        self.logger.info('starting warcprox')
        self.warcprox_controller.start()
        self.logger.info('starting chrome')
        self.browser()



    def shutdown(self):
        self.logger.info('shutting down warcprox')
        self.warcprox_controller.shutdown()
        time.sleep(5)
        os.killpg(os.getpgid(self.chrome_process.pid), signal.SIGTERM)


    def wait_for_shutdown_request(self):
        try:
            while not self.stop.is_set():
                time.sleep(0.5)
        finally:
            self.shutdown()

    def _warcprox_args(self, args):
        '''
        Takes args as produced by the argument parser built by
        _build_arg_parser and builds warcprox arguments object suitable to pass
        to warcprox.main.init_controller. Copies some arguments, renames some,
        populates some with defaults appropriate for dynacrawl, etc.
        '''
        warcprox_args = argparse.Namespace()
        warcprox_args.address = 'localhost'
        # let the OS choose an available port; discover it later using
        # sock.getsockname()[1]
        warcprox_args.port = 0
        warcprox_args.cacert = args.cacert
        warcprox_args.certs_dir = args.certs_dir
        warcprox_args.directory = args.warcs_dir
        warcprox_args.gzip = True
        warcprox_args.prefix = 'dynacrawl'
        warcprox_args.size = 1000 * 1000* 1000
        warcprox_args.rollover_idle_time = 3 * 60
        warcprox_args.digest_algorithm = 'sha1'
        warcprox_args.base32 = True
        warcprox_args.stats_db_file = None
        warcprox_args.playback_port = None
        warcprox_args.playback_index_db_file = None
        warcprox_args.rethinkdb_servers = None
        warcprox_args.rethinkdb_db = None
        warcprox_args.rethinkdb_big_table = None
        warcprox_args.dedup_db_file = None
        warcprox_args.kafka_broker_list = None
        warcprox_args.kafka_capture_feed_topic = None
        warcprox_args.queue_size = 500
        warcprox_args.max_threads = None
        warcprox_args.profile = False
        warcprox_args.onion_tor_socks_proxy = args.onion_tor_socks_proxy
        return warcprox_args

    def dump_state(self, signum=None, frame=None):
        state_strs = []
        for th in threading.enumerate():
            state_strs.append(str(th))
            stack = traceback.format_stack(sys._current_frames()[th.ident])
            state_strs.append(''.join(stack))
        logging.warn('dumping state (caught signal {})\n{}'.format(
            signum, '\n'.join(state_strs)))

def main(argv=None):
    argv = argv or sys.argv
    arg_parser = _build_arg_parser(argv)
    args = arg_parser.parse_args(args=argv[1:])
    configure_logging(args)
    controller = mainController(args)
    signal.signal(signal.SIGTERM, lambda a,b: controller.stop.set())
    signal.signal(signal.SIGINT, lambda a,b: controller.stop.set())
    signal.signal(signal.SIGQUIT, controller.dump_state)
    controller.start()
    #Give Chrome a little time to start
    time.sleep(5)
    controller.runJob()



if __name__ == '__main__':
    main()
