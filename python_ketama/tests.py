import ketama
import unittest
import os

class KetamaTest(unittest.TestCase):
    
    def setUp(self):
        self.valid_list_file = os.tmpnam()
        self.valid_list = file(self.valid_list_file, "w")
        self.valid_list.write("127.0.0.1:11211\t600\n")
        self.valid_list.write("127.0.0.1:11212\t400\n")
        self.valid_list.flush()
        self.invalid_list_file = os.tmpnam()
        self.invalid_list = file(self.invalid_list_file, "w")
        self.invalid_list.write("127.0.0.1:11211 600\n")
        self.invalid_list.write("127.0.0.1:11212 two\n")
        self.invalid_list.flush()

    def tearDown(self):
        self.valid_list.close()
        os.unlink(self.valid_list_file)
        self.invalid_list.close()
        os.unlink(self.invalid_list_file)

    def test_valid(self):
        cont = ketama.Continuum(self.valid_list_file)
        self.assertEqual(type(cont), ketama.Continuum)

    def test_invalid_null(self):
        self.assertRaises(ketama.KetamaError, ketama.Continuum, "/dev/null")

    def test_invalid_data(self):
        self.assertRaises(ketama.KetamaError, ketama.Continuum,
            self.invalid_list_file)

    def test_hashing(self):
        cont = ketama.Continuum(self.valid_list_file)
        self.assertEqual(cont.get_server("test"),
            (2959911115, '127.0.0.1:11211'))

    def test_hashi(self):
        self.assertEqual(ketama.hashi("test"), 2949673445)

    def test_adding(self):
        cont = ketama.Continuum(self.valid_list_file)
	old_count = cont.get_server_count()
        cont.add_server("127.0.0.1:11213", 700)
	self.assertEqual(cont.get_server_count(), old_count + 1)

    def test_server_count(self):
        cont = ketama.Continuum(self.valid_list_file)
        self.assertEqual(cont.get_server_count(), 3)

    def test_removal(self):
        cont = ketama.Continuum(self.valid_list_file)
        cont.remove_server("127.0.0.1:11211")
        self.assertTrue(1)

    def test_server_modified_count(self):
        cont = ketama.Continuum(self.valid_list_file)
        self.assertEqual(cont.get_server_count(), 3)


    def test_points(self):
        cont = ketama.Continuum(self.valid_list_file)
        self.assertEqual(len(cont.get_points()), 160 * 3)

if __name__ == "__main__":
    unittest.main()
