import argparse
import logging
import socket
import sys
import time
from pathlib import Path
from typing import List, Tuple
import datetime

class NoAcknowledgmentError(Exception):
    """ Exception raised when the server does not acknowledge the connection. """
    pass

class ConnectionError(Exception):
    """Exception raised for errors in establishing a connection."""
    pass

class ServerResponseError(Exception):
    """Exception raised for unexpected server responses."""
    pass

class ClusterDTester:
    def __init__(self, server_ip: str, server_port: int, \
                 mode: str = "fast", \
                 init_response: str = "+RCLUSTER Version v1.10"):
        """ Initialize the ClusterDTester with the server IP and port, the mode of 
        operation, and the expected initial response from the server on 
        connection. 

        Args:
        server_ip (str): The IP address of the server running the daemon service.
        server_port (int): The port number of the server.
        mode (str): Run mode: 'fast' for minimal logging and speed statistics,
                    'detailed' for a listing of clusters and duplicates.
        init_response (str): The expected initial response from the server on 
                             connection. Default is "+RCLUSTER Version v1.10".
        """
        self.server_ip = server_ip
        self.server_port = server_port
        self.mode = mode
        # make sure mode is either "fast" or "detailed"
        assert self.mode in ["fast", "detailed"], "Invalid mode. Use 'fast' or 'detailed'."
        self.init_response = init_response
        self.success_prefix = "+RCLUSTER"
        self.error_prefix = "-RCLUSTER"
        self.sock = None
        self.setup_logger()

    def setup_logger(self):
        """ set up the logger for ClusterDTester. """
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")
        log_filename = f"clusterd-log-{timestamp}.log"
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s',
                            filename=log_filename,
                            filemode='w')  # 'w' to overwrite the log file each time
        self.logger = logging.getLogger(__name__)
        # add console handler to the logger
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        # prevent the logger from propagating messages to the root logger
        self.logger.propagate = False

    def count_xml_files(self, directory: Path) -> Tuple[int, List[Path]]:
        """ count the number of XML files in a directory and return a list of the file paths."""
        xml_files = list(directory.glob("*.xml"))
        return len(xml_files), xml_files

    def establish_connection(self) -> socket.socket:
        """ establish a connection to the server. """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((self.server_ip, self.server_port))
            return sock
        except socket.error as e:
            raise ConnectionError(f"Failed to connect to {self.server_ip}:{self.server_port}: {e}")

    def validate_server_acknowledgment(self, sock):
        """ validate the server's acknowledgment of the connection by
        checking the initial response. """
        serv_init_response = sock.recv(1024).decode('utf-8')
        if serv_init_response != self.init_response:
            raise NoAcknowledgmentError(f"No acknowledgment from {self.server_ip}:{self.server_port}: {serv_init_response}")

    def establish_and_validate_connection(self):
        """ establish a connection to the server and validate the server's acknowledgment. """
        if self.sock is None:
            self.sock = self.establish_connection()
            self.validate_server_acknowledgment(self.sock)

    def send_rdf_data(self, sock, file_path):
        """ send the RDF data from a file to the server."""
        with file_path.open('rb') as file:
            rdf_data = file.read()
            sock.sendall(b"BUF" + rdf_data)

    def process_server_response(self, sock, file_path):
        """ process the server's response to the RDF data. """
        server_response = sock.recv(1024).decode('utf-8')
        if server_response.startswith(self.success_prefix) and self.mode == "detailed":
            self.logger.info(f"Success for {file_path}: {server_response}")
        elif server_response.startswith(self.error_prefix):
            error_message = server_response[len(self.error_prefix + ' '):]
            raise ServerResponseError(f"Failure for {file_path}: {error_message}")
        else:
            raise ServerResponseError(f"Unexpected response for {file_path}: {server_response}")

    def send_file(self, file_path: Path) -> Tuple[float, bool]:
        """ send an RDF file to the server and process the server's response. """
        start_time = time.time()
        success_flag = False
        try:
            self.send_rdf_data(self.sock, file_path)
            self.process_server_response(self.sock, file_path)
            success_flag = True
        except (ConnectionError, NoAcknowledgmentError, ServerResponseError) as e:
            self.logger.error(e)
            print(e, file=sys.stderr)
        return time.time() - start_time, success_flag

    def replay_xmlnews(self, directory: Path) -> None:
        """ open a directory of XMLNews stories and send them to the server for processing. """
        try:
            self.establish_and_validate_connection()
        except (ConnectionError, NoAcknowledgmentError) as e:
            self.logger.error(f"Initial connection failed: {e}")
            print(f"Initial connection failed: {e}", file=sys.stderr)
            self.sock.close()
            return

        _, xml_files = self.count_xml_files(directory)
        file_times = []
        error_count = 0

        for file_path in xml_files:
            if self.mode == "detailed":
                self.logger.info(f"Sending {file_path} to {self.server_ip}:{self.server_port}")

            time_taken, success = self.send_file(file_path)
            if success:
                file_times.append(time_taken)
            else:
                error_count += 1

        total_time = sum(file_times)
        success_file_count = len(file_times)
        self.logger.info(
            f"Processed {success_file_count} files with {error_count} errors in {total_time:.2f} seconds."
        )
        if success_file_count > 0:
            self.logger.info(
                f"Average processing time per successful file: {total_time / len(file_times):.2f} seconds."
            )

        self.sock.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test a daemon service by replaying XMLNews stories.")
    parser.add_argument("directory", help="The directory where XMLNews stories are stored.")
    parser.add_argument("server_ip", help="The IP address of the server running the daemon service.")
    parser.add_argument("server_port", type=int, help="The port number of the server.")
    parser.add_argument("--mode", choices=["fast", "detailed"], default="fast", help="Run mode: 'fast' for minimal logging and speed statistics, 'detailed' for a listing of clusters and duplicates.")
    args = parser.parse_args()

    tester = ClusterDTester(args.server_ip, args.server_port, args.mode)
    tester.replay_xmlnews(Path(args.directory))

# Run the script with the following command:
# python clusterd_tester.py /path/to/xmlnews/directory