from unittest import TestCase
from unittest.mock import MagicMock

import statwolf
from statwolf import Context
from statwolf.tempfile import TempFile

import tempfile

class FileMock:
    def __init__(self):
        self.name = 'a mock location'

    def write(self):
        pass

    def close(self):
        pass

class TempFileTestCase(TestCase):

    def setUp(self):
        self.tf = FileMock()
        tempfile.mkstemp = MagicMock(return_value=(3, 'a secure path'))
        self.openFile = MagicMock(return_value=self.tf)
        self.removeFile = MagicMock()

    def test_itShouldCreateATempFile(self):
        testFile = TempFile(tempfile, self.openFile, self.removeFile)

        tempfile.mkstemp.assert_called_with()
        self.openFile.assert_called_with('a secure path', 'w')
        self.assertEqual(testFile._tmpFile, self.tf)

    def test_itShouldReturnTheFileLocation(self):
        testFile = TempFile(tempfile, self.openFile, self.removeFile)
        self.assertEqual(testFile.location(), 'a mock location')

    def test_itShouldCloseTheFile(self):
        self.tf.close = MagicMock()

        testFile = TempFile(tempfile, self.openFile, self.removeFile)
        testFile.close()

        self.tf.close.assert_called_with()

    def test_itShouldWriteToTheFile(self):
        self.tf.write = MagicMock()

        testFile = TempFile(tempfile, self.openFile, self.removeFile)
        testFile.write('yolo')

        self.tf.write.assert_called_with(b'yolo')

    def test_itShouldRemoveAFile(self):
        testFile = TempFile(tempfile, self.openFile, self.removeFile)
        testFile.remove()

        self.removeFile.assert_called_with('a mock location')

    def test_itHasAFactoryMethod(self):
        tf = TempFile.create()
        self.assertIsInstance(tf, TempFile)
        tf.close()
        tf.remove()
