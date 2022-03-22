"""
Module to handle the encryption and decryption of private files
"""

import subprocess


class Encryption:
    """Handles the encryption and decryption of private files"""

    def __init__(self, key_path):
        """constructor: recives a unique parameter, the absolute path to the
                        identity file used for encryption and decryption"""
        self.key_path = key_path

    def encrypt(self, original_filename):
        """Takes a local file name and generates a new one, called the same
           plus the extension '.age', encrypted with the identity file.
           Returns the exit code of the executable."""
        result = subprocess.run(['age',
                                 '--encrypt',
                                 '--identity', self.key_path,
                                 '--output', original_filename + '.age',
                                 original_filename],
                                check=False)
        return result.returncode

    def decrypt(self, target_filename):
        """Takes a local file name ending in .age, and generates a new one,
           called as the given filename, decrypted with the identity file.
           Note the argument is the name of the file to be generated, not the
           imput file.
           Returns the exit code of the executable."""
        result = subprocess.run(['age',
                                 '--decrypt',
                                 '--identity', self.key_path,
                                 '--output', target_filename,
                                 target_filename + '.age'],
                                check=False)
        return result.returncode
