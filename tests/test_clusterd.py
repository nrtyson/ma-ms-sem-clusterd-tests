from sys import exc_info
import pytest
import socket
from pathlib import Path
from unittest.mock import mock_open, patch, MagicMock

from clusterd_tester import ClusterDTester, NoAcknowledgmentError, ConnectionError

@patch("socket.socket")
def test_send_file_success(mock_socket):
    """ mock the socket instance to simulate a successful connection and response """
    mock_sock_instance = MagicMock()
    mock_sock_instance.recv.side_effect = [b'+RCLUSTER Version v1.10', b'+RCLUSTER Success Message']
    mock_socket.return_value = mock_sock_instance

    # instantiate ClusterDTester
    tester = ClusterDTester("127.0.0.1", 12345, "detailed")
    tester.establish_and_validate_connection()

    # mock file opening and reading
    with patch("pathlib.Path.open", mock_open(read_data=b"Test RDF data")):
        file_path = Path("dummy_path.xml")  # dummy path for testing
        time_taken, success = tester.send_file(file_path)

    # assertions to check function behavior
    assert mock_socket.called
    mock_sock_instance.connect.assert_called_once_with(("127.0.0.1", 12345))
    mock_sock_instance.sendall.assert_called_once_with(b"BUFTest RDF data")
    assert success
    assert time_taken >= 0.0

@patch("socket.socket")
def test_no_acknowledgment_error(mock_socket):
    """ simulate no acknowledgment error exception """ 
    mock_sock_instance = MagicMock()
    mock_sock_instance.recv.return_value = b'Wrong Response'
    mock_socket.return_value = mock_sock_instance

    tester = ClusterDTester("127.0.0.1", 12345, "detailed")
    
    with pytest.raises(NoAcknowledgmentError):
        tester.establish_and_validate_connection()

@patch("socket.socket")
def test_server_response_error(mock_socket):
    """ similate server response error """
    mock_sock_instance = MagicMock()
    mock_sock_instance.recv.side_effect = [b'+RCLUSTER Version v1.10', 
                                           b'-RCLUSTER (100) Service Unavailable']
    mock_socket.return_value = mock_sock_instance

    tester = ClusterDTester("127.0.0.1", 12345, "detailed")
    tester.establish_and_validate_connection()
    
    with patch("pathlib.Path.open", mock_open(read_data=b"Test RDF data")):
        file_path = Path("dummy_path.xml")
        time_taken, success = tester.send_file(file_path)

    assert not success
    assert time_taken >= 0.0

@patch("socket.socket")
def test_connection_error(mock_socket):
    """ simulate a connection error scenario. """
    mock_socket.side_effect = socket.error

    tester = ClusterDTester("127.0.0.1", 12345, "detailed")

    with pytest.raises(ConnectionError):
        tester.establish_connection()
