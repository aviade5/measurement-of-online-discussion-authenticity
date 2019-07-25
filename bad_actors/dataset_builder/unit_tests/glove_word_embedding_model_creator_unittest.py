from __future__ import print_function
import unittest
from DB.schema_definition import *
from dataset_builder.word_embedding.glove_word_embedding_model_creator import GloveWordEmbeddingModelCreator
import numpy as np

class GloveWordEmbeddingModelCreatorUnittest(unittest.TestCase):
    def setUp(self):
        self._config_parser = getConfig()
        self._db = DB()
        self._db.setUp()

        # self._Word_Embedding_Model_Creator.execute(None)
        self._is_load_wikipedia_300d_glove_model = True
        self._wikipedia_model_file_path = "data/input/glove/test_glove.6B.300d_small.txt"
        self._table_name = "wikipedia_model_300d"
        self._word_vector_dict_full_path = "data/output/word_embedding/"
        self._word_vector_dict = {}

        dim = 300
        self._author_embedding = u'author_word_embeddings_%s_%s_dim' % (self._table_name, dim)

        self._author = None
        self._set_author(u'test_user')
        self._counter = 0
        self._posts = []

    def tearDown(self):
        self._db.session.close()
        self._db.deleteDB()
        self._db.session.close()

    def test_case_basic(self):
        self._add_post(u'the of to the', u'about of for')
        self._setup_test()
        expected_values = self._calc_results()
        self._generic_test(expected_values)

    def test_no_word(self):
        self._add_post(u'Only title', u'')
        self._setup_test()
        expected_value = self._calc_results()
        self._generic_test(expected_value)

    def test_one_empty_word(self):
        self._add_post(u'Post1', u'')
        self._add_post(u'post2', u'about')
        self._setup_test()
        expected_value = self._calc_results()
        self._generic_test(expected_value)

    def test_space_sentence(self):
        self._add_post(u'Post1', u' ')
        self._add_post(u'post2', u'for')
        self._setup_test()
        expected_value = self._calc_results()
        self._generic_test(expected_value)

    def test__few_words_in_one_post(self):
        self._add_post(u'Post3', u'This is a test post')
        self._setup_test()
        expected_value = self._calc_results()
        self._generic_test(expected_value)

    def test_word_cleaning_in_one_post_dirty(self):
        self._add_post(u'Post3', u'of!@#!@$!@$$')
        self._setup_test()
        expected_value = self._calc_results()
        self._generic_non_equal_test(expected_value)

    def test_word_cleanin_in_one_post_clean(self):
        self._add_post(u'Post3', u'of')
        self._setup_test()
        expected_value = self._calc_results()
        self._generic_test(expected_value)

    def test_nonsense_posts(self):
        self._add_post(u'Post1', u'sdakfalfhlag!@$!@%!%@')
        self._add_post(u'Post2', u'frarararararara')
        self._add_post(u'Post3', u'!@$!@%!%@')
        self._setup_test()
        expected_value = self._calc_results()
        self._generic_test(expected_value)

    def test_one_word_in_dict_per_post(self):
        self._add_post(u'Post1', u'the')
        self._add_post(u'Post2', u'and')
        self._add_post(u'Post3', u'said ')
        self._setup_test()
        expected_value = self._calc_results()
        self._generic_test(expected_value)

    def test_one_word_in_dict_per_post_and_empty_post(self):
        self._add_post(u'Post1', u'the')
        self._add_post(u'', u'')
        self._setup_test()
        expected_value = self._calc_results()
        self._generic_test(expected_value)

    def test_word_value_correctnes(self):
        self._add_post(u'Post1', u'that')
        self._setup_test()
        expected_value = (-0.18256, 0.49851, -0.1639, -0.17443, -0.16382, -0.044109, 0.27957, 0.066851, 0.12298, -2.4794, 0.24149, 0.18546, -0.18717, 0.079715, 0.33302, 0.42865, -0.041367, -0.16182, 0.050724, 0.045087, 0.22453, 0.61712, 0.32051, -0.032104, -0.39375, 0.0014437, -0.094092, -0.26371, -0.35796, 0.094285, 0.13893, 0.65392, -0.28118, -0.26782, -0.78944, 0.15803, -0.10546, 0.12397, 0.20667, -0.15379, -0.057678, -0.24583, -0.26697, 0.2152, -0.064428, 0.31629, -0.17376, -0.074701, -0.36648, -0.02686, -0.1075, 0.10345, -0.017767, 0.042695, -0.017136, 0.35075, -0.13328, 0.36784, 0.12393, -0.04653, 0.037204, 0.29934, 0.4952, 0.12204, -0.27603, -0.3364, 0.058922, 0.15802, -0.0056367, 0.21791, 0.11707, -0.04361, 0.008349, 0.0070214, 0.043938, 0.11344, 0.33828, 0.15325, -0.58786, 0.46672, -0.12652, -0.19122, 0.25451, 0.22502, 0.14879, 0.092189, -0.13985, 0.27358, 0.23251, 0.26713, -0.44398, 0.40976, -0.51229, -0.14665, -0.01102, 0.071824, -0.26319, 0.27244, 0.12643, -0.37083, 0.3596, 0.13582, 0.12482, -0.13916, 0.11157, 0.3108, -0.051193, -0.11273, -0.45689, 0.34491, -0.14823, -0.137, 0.12242, -0.15427, 0.21486, 0.25317, -0.2041, 0.081754, 0.03881, -0.40583, 0.16871, -0.2299, 0.20674, 0.23063, 0.24366, -0.18274, -0.062705, 0.12812, -0.064516, 0.074334, 0.12669, 0.11789, 0.088576, -0.1372, -0.01106, -0.012716, -0.0035247, -0.022579, 0.18878, -0.11215, 0.00090086, 0.0090595, 0.2356, -0.043798, -0.45454, -0.29099, 0.18257, 0.10359, -0.29622, 0.20828, -0.13141, 0.054049, 0.13739, -0.040951, 0.35176, 0.19027, 0.013783, 0.032382, 0.054266, -0.20215, 0.44911, -0.27652, 0.14964, -0.077348, -0.023959, 0.076235, 0.075218, 0.14075, -0.36507, 0.20819, 0.063611, -0.1407, -0.66511, 0.059657, -0.14432, -0.17164, -0.39068, 0.26287, -0.0036708, 0.29413, 0.31535, 0.48286, 0.46871, -0.22415, -0.11031, -0.0020065, -0.32997, 0.26679, 0.092457, 0.42964, -0.28678, 0.08467, 0.011193, -0.050083, 0.084792, -0.017398, 0.11529, 0.18289, -0.27396, 0.022409, 0.72348, -0.42307, -0.06354, -0.050153, -0.12046, 0.008331, -0.073412, 0.069604, -0.24859, -0.2272, -0.15613, 0.022409, 0.67996, -0.032504, -0.051117, -0.04591, -0.0078706, 0.19829, -0.061915, -0.009928, 0.51408, -0.22065, -0.28594, 0.067643, 0.17443, 0.25221, 0.13774, 0.078802, -0.0091473, -0.16622, -0.09775, 0.066662, 0.11759, -0.13983, -0.50508, 0.12119, 0.089406, -0.21979, 0.048579, -0.0057536, 0.47699, -0.21758, 0.18851, 0.021163, -0.6977, -0.16276, 0.053694, -0.41053, -0.09164, -0.14957, -0.12347, -0.13177, -0.22842, -0.043611, 0.52772, -0.13661, -0.11012, -0.13102, 0.279, -0.18485, 0.037604, -0.026723, 0.32183, 0.035192, -0.054587, -0.046015, -0.069506, 0.023502, 0.041348, 0.088307, -0.12754, 0.31563, -0.17489, 0.38135, 0.35272, 0.11312, -2.3311, -0.084484, 0.65596, 0.34984, -0.040833, 0.26918, 0.077779, 0.23471, -0.18731, -0.027106, -0.18765, 0.1666, -0.15811, 0.12011, -0.082465, -0.23241, 0.022031, 0.35445, 0.17221, 0.018176, 0.038145, -0.27224, -0.19107, -0.094104)
        db_value = self._words[u'that']
        self.assertEquals(expected_value, db_value)

    def test_two_words_value_correctness(self):
        self._add_post(u'Post1', u'is was')
        self._setup_test()
        is_word_vec = (-0.1749, 0.22956, 0.24924, -0.20512, -0.12294, 0.021297, -0.23815, 0.13737, -0.08913, -2.0607, 0.35843, -0.20365, -0.015518, 0.25628, 0.22963, 0.0011985, -0.89833, 0.13609, 0.18861, -0.33359, 0.018397, 0.62946, -0.13167, 0.64819, -0.2175, 0.093853, -0.03905, -0.50846, -0.2554, 0.32361, 0.23231, 0.49105, -0.41841, 0.073934, -0.65639, 0.48608, -0.11219, -0.29994, -0.72501, 0.085377, -0.050447, 0.23105, -0.064843, 0.0039056, 0.099742, -0.020334, 0.38845, 0.24464, -0.086308, -0.11308, 0.019281, -0.11205, 0.065642, 0.1812, -0.10949, 0.055968, -0.197, 0.49184, 0.61818, -0.03319, 0.073289, -0.022823, 0.66946, 0.18233, -0.40082, -0.33717, -0.28521, -0.28222, -0.044373, 0.14881, -0.42135, 0.051545, 0.27605, -0.19959, -0.29766, -0.087712, 0.4621, 0.16891, -0.19415, 0.28327, -0.25327, -0.063275, 0.090945, -0.18623, 0.28891, 0.043534, -0.10303, 0.39545, 0.088457, 0.054829, -0.45487, 0.38226, 0.15458, -0.42001, 0.20908, 0.0010261, -0.37166, 0.28856, -0.0072666, -0.23869, 0.18698, 0.21457, 0.0024625, -0.22166, -0.10549, 0.26366, 0.63795, -0.21856, -0.02476, 0.26296, -0.10332, -0.2183, -0.39545, -0.2069, 0.5254, -0.24287, -0.12601, -0.1683, -0.1337, -0.17463, -0.24256, 0.16015, -0.25534, -0.028585, 0.24915, -0.45177, 0.77189, 0.037554, 0.15222, -0.059098, 0.096185, -0.0036737, 0.23823, -0.21157, -0.10788, 0.10368, 0.069922, 0.078669, -0.017694, 0.14711, -0.1157, 0.3188, -0.16336, -0.016621, -0.33311, -0.57466, 0.15262, 0.037973, -0.3337, 0.29795, 0.27213, -0.47173, -0.0049383, 0.15796, 0.42384, -0.018251, 0.22948, -0.02412, -0.13726, 0.42183, 0.25098, -0.24748, 0.024097, -0.23881, -0.045085, -0.11757, 0.024051, 0.072332, 0.0023528, 0.04958, 0.32707, 0.13398, -0.86314, 0.16043, -0.014522, 0.17027, -0.53185, -0.44085, -0.11198, 0.16672, -0.1007, -0.13035, 0.035605, -0.015818, 0.1897, -0.35454, -0.12724, -0.28169, -0.12438, 0.45235, 0.13082, 0.47829, 0.11159, 0.20746, -0.55962, -0.018372, -0.10443, -0.45267, 0.098301, -0.28683, 1.2324, -0.0077764, -0.1972, -0.35824, 0.098736, 0.010953, -0.023358, -0.26791, -0.082025, -0.21353, 0.050866, -0.5375, 0.20151, -0.10377, 0.19563, 0.31699, -0.27754, -0.08434, -0.47337, 0.16286, -0.049013, -0.054793, -0.079239, -0.010989, 0.52198, -0.14411, 0.045905, 0.32172, -0.039952, -0.19748, 0.35732, -0.083146, 0.30254, 0.45123, -0.0075578, 0.14041, -0.073527, -0.0002206, -0.17764, 0.35673, 0.23883, 0.15452, 0.5127, -0.12761, -0.8158, 0.079303, 0.46277, 0.25961, 0.34596, -0.41715, 0.082716, 0.14205, -0.57492, -0.076417, -0.089484, -0.097053, -0.32071, -0.34892, 0.24133, -0.29925, -0.091908, 0.20075, 0.33906, 0.2012, -0.40629, 0.027334, 0.30098, 0.099071, 0.63656, 0.050048, -0.19518, -0.41454, -0.16104, 0.20477, -0.14608, 0.36813, -1.7321, -0.29467, 0.53281, 0.14033, 0.11016, -0.14307, -0.33054, 0.096295, -0.30065, 0.0887, -0.33432, 0.25402, 0.1337, 0.28222, 0.31357, -0.13407, 0.18465, 0.23426, 0.076272, 0.10502, 0.21521, -0.24131, -0.40402, 0.054744)
        was_word_vec = (0.065573, 0.022011, -0.13182, -0.2133, -0.045275, -0.095786, -0.19706, 0.0082058, -0.29285, -1.823, 0.13935, 0.050812, 0.096161, 0.4148, 0.26364, 0.68439, 0.11008, 0.049237, -0.28425, -0.40207, -0.14409, 0.2281, -0.30332, 0.14454, -0.43272, 0.036927, -0.013514, -0.54728, 0.20058, 0.10093, -0.039715, 0.20683, 0.026177, 0.67707, -0.86194, -0.020663, -0.10421, 0.19081, 0.19231, 0.45219, -0.13793, -0.24603, 0.28813, 0.25795, 0.36604, 0.3431, -0.027197, 0.34379, 0.039582, -0.078206, -0.064917, 0.056657, -0.17806, -0.12867, 0.027848, -0.0021481, -0.055731, 0.33005, 0.64831, -0.2476, -0.073944, 0.17024, 0.17648, -0.13079, -0.17363, -0.46197, -0.030292, -0.48792, -0.051405, -0.1089, 0.040095, 0.20205, -0.070271, 0.0487, -0.10198, -0.082109, 0.60098, 0.37644, -0.11082, 0.28375, -0.19218, -0.028718, 0.2886, 0.36103, 0.28356, -0.11606, -0.18276, 0.35638, 0.39098, 0.18356, -0.068084, -0.24342, -0.13919, 0.010534, 0.29407, -0.2509, -0.00093653, 0.047333, -0.11968, -0.25735, -0.10426, 0.0011458, 0.08289, 0.069742, 0.43394, 0.099985, -0.076839, 0.3029, -0.072626, 0.22189, -0.28636, 0.37533, 0.10435, -0.25002, 0.072788, -0.57388, 0.14683, -0.55855, 0.14287, -0.44197, -0.39824, -0.0044806, 0.023105, -0.40818, 0.031007, -0.17876, 0.023245, 0.099767, -0.15579, 0.22145, 0.0040429, 0.37884, 0.24359, -0.27678, -0.45042, 0.18068, -0.00070251, -0.23596, -0.14223, 0.23056, -0.14667, 0.14092, 0.083994, 0.10351, -0.21659, -0.07533, 0.60886, 0.10209, -0.38598, -0.25118, 0.55, -0.25347, 0.026186, 0.31511, 0.0056063, -0.13066, 0.069233, 0.27206, 0.24878, 0.19029, -0.20216, -0.33764, -0.12913, 0.12183, 0.41517, -0.20679, -0.001865, 0.46737, 0.66421, 0.2779, 0.41703, -0.38389, -0.50318, -0.30409, 0.0061397, 0.37694, 0.18032, -0.093641, 0.15974, 0.37913, 0.36016, 0.65961, 0.042113, 0.15859, 0.06626, -0.48581, -0.61004, -0.019813, 0.23744, 0.4306, 0.020782, 0.19697, 0.054576, -0.25317, -0.22784, -0.58141, -0.69161, -0.18103, 0.0024407, -0.43854, 0.83509, -0.20984, -0.064139, -0.2284, 0.12102, 0.13029, 0.13414, 0.18592, 0.26716, 0.099083, -0.089311, -0.12209, -0.16353, -0.067562, -0.073342, -0.034916, -0.18393, 0.04132, 0.46833, 0.15102, 0.2258, -0.47783, -0.11545, 0.071741, 0.20792, 0.44219, 0.11758, -0.59384, 0.070048, 0.047424, 0.086764, -0.30923, -0.36463, -0.37644, -0.058562, -0.30689, -0.035164, 0.1929, 0.14827, 0.48928, -0.037628, -0.022275, 0.3316, 0.37314, -0.5712, -0.036131, 0.68265, -0.29565, 0.31916, 0.03892, 0.28439, -0.35364, -0.45618, -0.10522, 0.89174, -0.07723, -0.39188, -0.1909, 0.50079, 0.026101, -0.41087, -0.0026623, -0.43731, 0.1219, -0.3406, 0.019426, 0.18329, -0.0035909, 0.094648, 0.075009, -0.29272, -0.94985, 0.17033, -0.15582, -0.059861, 0.18346, -1.8983, -0.12707, 0.16523, 0.33603, -0.64783, 0.2662, 0.053649, -0.26753, 0.13939, 0.30099, -0.65434, -0.088767, -0.14839, -0.040554, 0.34577, -0.22928, 0.24341, 0.33654, 0.29751, 0.44617, 0.30077, -0.21916, -0.43186, -0.080348)
        self._words_vectors = [is_word_vec, was_word_vec]
        expected_val = self._calc_results()

        self._generic_test(expected_val)

    def test_two_words_in_two_posts_value_correctness(self):
        self._add_post(u'Post1', u'is')
        self._add_post(u'Post2', u'was')
        self._setup_test()
        is_word_vec = (-0.1749, 0.22956, 0.24924, -0.20512, -0.12294, 0.021297, -0.23815, 0.13737, -0.08913, -2.0607, 0.35843, -0.20365, -0.015518, 0.25628, 0.22963, 0.0011985, -0.89833, 0.13609, 0.18861, -0.33359, 0.018397, 0.62946, -0.13167, 0.64819, -0.2175, 0.093853, -0.03905, -0.50846, -0.2554, 0.32361, 0.23231, 0.49105, -0.41841, 0.073934, -0.65639, 0.48608, -0.11219, -0.29994, -0.72501, 0.085377, -0.050447, 0.23105, -0.064843, 0.0039056, 0.099742, -0.020334, 0.38845, 0.24464, -0.086308, -0.11308, 0.019281, -0.11205, 0.065642, 0.1812, -0.10949, 0.055968, -0.197, 0.49184, 0.61818, -0.03319, 0.073289, -0.022823, 0.66946, 0.18233, -0.40082, -0.33717, -0.28521, -0.28222, -0.044373, 0.14881, -0.42135, 0.051545, 0.27605, -0.19959, -0.29766, -0.087712, 0.4621, 0.16891, -0.19415, 0.28327, -0.25327, -0.063275, 0.090945, -0.18623, 0.28891, 0.043534, -0.10303, 0.39545, 0.088457, 0.054829, -0.45487, 0.38226, 0.15458, -0.42001, 0.20908, 0.0010261, -0.37166, 0.28856, -0.0072666, -0.23869, 0.18698, 0.21457, 0.0024625, -0.22166, -0.10549, 0.26366, 0.63795, -0.21856, -0.02476, 0.26296, -0.10332, -0.2183, -0.39545, -0.2069, 0.5254, -0.24287, -0.12601, -0.1683, -0.1337, -0.17463, -0.24256, 0.16015, -0.25534, -0.028585, 0.24915, -0.45177, 0.77189, 0.037554, 0.15222, -0.059098, 0.096185, -0.0036737, 0.23823, -0.21157, -0.10788, 0.10368, 0.069922, 0.078669, -0.017694, 0.14711, -0.1157, 0.3188, -0.16336, -0.016621, -0.33311, -0.57466, 0.15262, 0.037973, -0.3337, 0.29795, 0.27213, -0.47173, -0.0049383, 0.15796, 0.42384, -0.018251, 0.22948, -0.02412, -0.13726, 0.42183, 0.25098, -0.24748, 0.024097, -0.23881, -0.045085, -0.11757, 0.024051, 0.072332, 0.0023528, 0.04958, 0.32707, 0.13398, -0.86314, 0.16043, -0.014522, 0.17027, -0.53185, -0.44085, -0.11198, 0.16672, -0.1007, -0.13035, 0.035605, -0.015818, 0.1897, -0.35454, -0.12724, -0.28169, -0.12438, 0.45235, 0.13082, 0.47829, 0.11159, 0.20746, -0.55962, -0.018372, -0.10443, -0.45267, 0.098301, -0.28683, 1.2324, -0.0077764, -0.1972, -0.35824, 0.098736, 0.010953, -0.023358, -0.26791, -0.082025, -0.21353, 0.050866, -0.5375, 0.20151, -0.10377, 0.19563, 0.31699, -0.27754, -0.08434, -0.47337, 0.16286, -0.049013, -0.054793, -0.079239, -0.010989, 0.52198, -0.14411, 0.045905, 0.32172, -0.039952, -0.19748, 0.35732, -0.083146, 0.30254, 0.45123, -0.0075578, 0.14041, -0.073527, -0.0002206, -0.17764, 0.35673, 0.23883, 0.15452, 0.5127, -0.12761, -0.8158, 0.079303, 0.46277, 0.25961, 0.34596, -0.41715, 0.082716, 0.14205, -0.57492, -0.076417, -0.089484, -0.097053, -0.32071, -0.34892, 0.24133, -0.29925, -0.091908, 0.20075, 0.33906, 0.2012, -0.40629, 0.027334, 0.30098, 0.099071, 0.63656, 0.050048, -0.19518, -0.41454, -0.16104, 0.20477, -0.14608, 0.36813, -1.7321, -0.29467, 0.53281, 0.14033, 0.11016, -0.14307, -0.33054, 0.096295, -0.30065, 0.0887, -0.33432, 0.25402, 0.1337, 0.28222, 0.31357, -0.13407, 0.18465, 0.23426, 0.076272, 0.10502, 0.21521, -0.24131, -0.40402, 0.054744)
        was_word_vec = (0.065573, 0.022011, -0.13182, -0.2133, -0.045275, -0.095786, -0.19706, 0.0082058, -0.29285, -1.823, 0.13935, 0.050812, 0.096161, 0.4148, 0.26364, 0.68439, 0.11008, 0.049237, -0.28425, -0.40207, -0.14409, 0.2281, -0.30332, 0.14454, -0.43272, 0.036927, -0.013514, -0.54728, 0.20058, 0.10093, -0.039715, 0.20683, 0.026177, 0.67707, -0.86194, -0.020663, -0.10421, 0.19081, 0.19231, 0.45219, -0.13793, -0.24603, 0.28813, 0.25795, 0.36604, 0.3431, -0.027197, 0.34379, 0.039582, -0.078206, -0.064917, 0.056657, -0.17806, -0.12867, 0.027848, -0.0021481, -0.055731, 0.33005, 0.64831, -0.2476, -0.073944, 0.17024, 0.17648, -0.13079, -0.17363, -0.46197, -0.030292, -0.48792, -0.051405, -0.1089, 0.040095, 0.20205, -0.070271, 0.0487, -0.10198, -0.082109, 0.60098, 0.37644, -0.11082, 0.28375, -0.19218, -0.028718, 0.2886, 0.36103, 0.28356, -0.11606, -0.18276, 0.35638, 0.39098, 0.18356, -0.068084, -0.24342, -0.13919, 0.010534, 0.29407, -0.2509, -0.00093653, 0.047333, -0.11968, -0.25735, -0.10426, 0.0011458, 0.08289, 0.069742, 0.43394, 0.099985, -0.076839, 0.3029, -0.072626, 0.22189, -0.28636, 0.37533, 0.10435, -0.25002, 0.072788, -0.57388, 0.14683, -0.55855, 0.14287, -0.44197, -0.39824, -0.0044806, 0.023105, -0.40818, 0.031007, -0.17876, 0.023245, 0.099767, -0.15579, 0.22145, 0.0040429, 0.37884, 0.24359, -0.27678, -0.45042, 0.18068, -0.00070251, -0.23596, -0.14223, 0.23056, -0.14667, 0.14092, 0.083994, 0.10351, -0.21659, -0.07533, 0.60886, 0.10209, -0.38598, -0.25118, 0.55, -0.25347, 0.026186, 0.31511, 0.0056063, -0.13066, 0.069233, 0.27206, 0.24878, 0.19029, -0.20216, -0.33764, -0.12913, 0.12183, 0.41517, -0.20679, -0.001865, 0.46737, 0.66421, 0.2779, 0.41703, -0.38389, -0.50318, -0.30409, 0.0061397, 0.37694, 0.18032, -0.093641, 0.15974, 0.37913, 0.36016, 0.65961, 0.042113, 0.15859, 0.06626, -0.48581, -0.61004, -0.019813, 0.23744, 0.4306, 0.020782, 0.19697, 0.054576, -0.25317, -0.22784, -0.58141, -0.69161, -0.18103, 0.0024407, -0.43854, 0.83509, -0.20984, -0.064139, -0.2284, 0.12102, 0.13029, 0.13414, 0.18592, 0.26716, 0.099083, -0.089311, -0.12209, -0.16353, -0.067562, -0.073342, -0.034916, -0.18393, 0.04132, 0.46833, 0.15102, 0.2258, -0.47783, -0.11545, 0.071741, 0.20792, 0.44219, 0.11758, -0.59384, 0.070048, 0.047424, 0.086764, -0.30923, -0.36463, -0.37644, -0.058562, -0.30689, -0.035164, 0.1929, 0.14827, 0.48928, -0.037628, -0.022275, 0.3316, 0.37314, -0.5712, -0.036131, 0.68265, -0.29565, 0.31916, 0.03892, 0.28439, -0.35364, -0.45618, -0.10522, 0.89174, -0.07723, -0.39188, -0.1909, 0.50079, 0.026101, -0.41087, -0.0026623, -0.43731, 0.1219, -0.3406, 0.019426, 0.18329, -0.0035909, 0.094648, 0.075009, -0.29272, -0.94985, 0.17033, -0.15582, -0.059861, 0.18346, -1.8983, -0.12707, 0.16523, 0.33603, -0.64783, 0.2662, 0.053649, -0.26753, 0.13939, 0.30099, -0.65434, -0.088767, -0.14839, -0.040554, 0.34577, -0.22928, 0.24341, 0.33654, 0.29751, 0.44617, 0.30077, -0.21916, -0.43186, -0.080348)
        self._words_vectors = [is_word_vec, was_word_vec]
        expected_val = self._calc_results()

        self._generic_test(expected_val)

    def test_where_clause_effect(self):
        self._add_post(u'Post1', u'was')
        self._add_post(u'Post2', u'was')
        self._add_target_article(0, u'Article1', u'Desc1', u'test_user')
        self._add_target_article_item(0, u'paragraph', u'is', u'test_user')
        self._add_target_article_item(1, u'content', u'was', u'test_user')
        is_word_vec = (-0.1749, 0.22956, 0.24924, -0.20512, -0.12294, 0.021297, -0.23815, 0.13737, -0.08913, -2.0607, 0.35843, -0.20365, -0.015518, 0.25628, 0.22963, 0.0011985, -0.89833, 0.13609, 0.18861, -0.33359, 0.018397, 0.62946, -0.13167, 0.64819, -0.2175, 0.093853, -0.03905, -0.50846, -0.2554, 0.32361, 0.23231, 0.49105, -0.41841, 0.073934, -0.65639, 0.48608, -0.11219, -0.29994, -0.72501, 0.085377, -0.050447, 0.23105, -0.064843, 0.0039056, 0.099742, -0.020334, 0.38845, 0.24464, -0.086308, -0.11308, 0.019281, -0.11205, 0.065642, 0.1812, -0.10949, 0.055968, -0.197, 0.49184, 0.61818, -0.03319, 0.073289, -0.022823, 0.66946, 0.18233, -0.40082, -0.33717, -0.28521, -0.28222, -0.044373, 0.14881, -0.42135, 0.051545, 0.27605, -0.19959, -0.29766, -0.087712, 0.4621, 0.16891, -0.19415, 0.28327, -0.25327, -0.063275, 0.090945, -0.18623, 0.28891, 0.043534, -0.10303, 0.39545, 0.088457, 0.054829, -0.45487, 0.38226, 0.15458, -0.42001, 0.20908, 0.0010261, -0.37166, 0.28856, -0.0072666, -0.23869, 0.18698, 0.21457, 0.0024625, -0.22166, -0.10549, 0.26366, 0.63795, -0.21856, -0.02476, 0.26296, -0.10332, -0.2183, -0.39545, -0.2069, 0.5254, -0.24287, -0.12601, -0.1683, -0.1337, -0.17463, -0.24256, 0.16015, -0.25534, -0.028585, 0.24915, -0.45177, 0.77189, 0.037554, 0.15222, -0.059098, 0.096185, -0.0036737, 0.23823, -0.21157, -0.10788, 0.10368, 0.069922, 0.078669, -0.017694, 0.14711, -0.1157, 0.3188, -0.16336, -0.016621, -0.33311, -0.57466, 0.15262, 0.037973, -0.3337, 0.29795, 0.27213, -0.47173, -0.0049383, 0.15796, 0.42384, -0.018251, 0.22948, -0.02412, -0.13726, 0.42183, 0.25098, -0.24748, 0.024097, -0.23881, -0.045085, -0.11757, 0.024051, 0.072332, 0.0023528, 0.04958, 0.32707, 0.13398, -0.86314, 0.16043, -0.014522, 0.17027, -0.53185, -0.44085, -0.11198, 0.16672, -0.1007, -0.13035, 0.035605, -0.015818, 0.1897, -0.35454, -0.12724, -0.28169, -0.12438, 0.45235, 0.13082, 0.47829, 0.11159, 0.20746, -0.55962, -0.018372, -0.10443, -0.45267, 0.098301, -0.28683, 1.2324, -0.0077764, -0.1972, -0.35824, 0.098736, 0.010953, -0.023358, -0.26791, -0.082025, -0.21353, 0.050866, -0.5375, 0.20151, -0.10377, 0.19563, 0.31699, -0.27754, -0.08434, -0.47337, 0.16286, -0.049013, -0.054793, -0.079239, -0.010989, 0.52198, -0.14411, 0.045905, 0.32172, -0.039952, -0.19748, 0.35732, -0.083146, 0.30254, 0.45123, -0.0075578, 0.14041, -0.073527, -0.0002206, -0.17764, 0.35673, 0.23883, 0.15452, 0.5127, -0.12761, -0.8158, 0.079303, 0.46277, 0.25961, 0.34596, -0.41715, 0.082716, 0.14205, -0.57492, -0.076417, -0.089484, -0.097053, -0.32071, -0.34892, 0.24133, -0.29925, -0.091908, 0.20075, 0.33906, 0.2012, -0.40629, 0.027334, 0.30098, 0.099071, 0.63656, 0.050048, -0.19518, -0.41454, -0.16104, 0.20477, -0.14608, 0.36813, -1.7321, -0.29467, 0.53281, 0.14033, 0.11016, -0.14307, -0.33054, 0.096295, -0.30065, 0.0887, -0.33432, 0.25402, 0.1337, 0.28222, 0.31357, -0.13407, 0.18465, 0.23426, 0.076272, 0.10502, 0.21521, -0.24131, -0.40402, 0.054744)
        expected_value = is_word_vec
        self._setup_test()
        db_results = self._db.get_author_word_embedding(u'0', u'target_article_items', u"_type_paragraph_content",self._author_embedding) # check if this is ok with Aviad
        db_results = db_results[u'min']
        self.assertEquals(expected_value, db_results)

    def test_add_additional_fields_to_existing_table(self):
        self._add_post(u'was', u'is')
        self._add_post(u'is', u'was')
        self._db.session.commit()
        self._word_embedding_model_creator = GloveWordEmbeddingModelCreator(self._db)

        self._word_embedding_model_creator.execute(None)
        self._word_embedding_model_creator._aggregation_functions_names = ['sum']
        self._word_embedding_model_creator.execute(None)
        db_results = self._db.get_author_word_embedding(u'test_user', u'posts','title',self._author_embedding)  # check if
        #  this is ok with Aviad
        try:
            if db_results[u'sum'] is not None and db_results[u'np.mean'] is not None:
                self.assertTrue(True)
            else:
                self.assertTrue(False)
        except:
            self.assertTrue(False)

    def test_case_post_represent_by_posts(self):
        self._add_post(u'post1', u'the claim', u'Claim')
        self._add_post(u'post2', u'dog cat pig man')  # 2
        self._add_post(u'post3', u'TV is the best guys')  # 1
        self._add_claim_tweet_connection(u'post1', u'post2')
        self._add_claim_tweet_connection(u'post1', u'post3')
        self._db.session.commit()
        self._word_embedding_model_creator = GloveWordEmbeddingModelCreator(self._db)
        self._word_embedding_model_creator._targeted_fields_for_embedding = [{
            'source': {'table_name': 'posts', 'id': 'post_id'},
            'connection': {'table_name': 'claim_tweet_connection', 'source_id': 'claim_id', 'target_id': 'post_id'},
            'destination': {'table_name': 'posts', 'id': 'post_id', 'target_field': 'content',
                            "where_clauses": [{"field_name": 1, "value": 1}]}}]

        self._word_embedding_model_creator.execute(None)
        self._words = self._db.get_word_embedding_dictionary()
        self._words_vectors = self._get_posts_val()
        expected_val = self._calc_results()
        self._generic_test(expected_val, u'post1')

    def _setup_test(self):
        self._db.session.commit()
        self._word_embedding_model_creator = GloveWordEmbeddingModelCreator(self._db)
        self._word_embedding_model_creator.execute(None)

        self._words = self._db.get_word_embedding_dictionary()
        self._words_vectors = self._get_posts_val()

    def _generic_test(self, expected_value, source_id=u""):
        if source_id == u"":
            source_id = self._author.author_guid

        db_results = self._db.get_author_word_embedding(source_id, u'posts', u'content', self._author_embedding)
        self.assertEquals(expected_value[u'min'], db_results[u'min'])
        self.assertEquals(expected_value[u'max'], db_results[u'max'])
        self.assertEquals(expected_value[u'np.mean'], db_results[u'np.mean'])

    def _generic_non_equal_test(self, expected_value):
        db_results = self._db.get_author_word_embedding(self._author.author_guid, u'posts', u'content',self._author_embedding)
        self.assertNotEqual(expected_value[u'min'], db_results[u'min'])
        self.assertNotEqual(expected_value[u'max'], db_results[u'max'])
        self.assertNotEqual(expected_value[u'np.mean'], db_results[u'np.mean'])

    def _set_author(self, author_guid):
        author = Author()
        author.author_guid = author_guid
        author.author_full_name = u'name'+author_guid
        author.name = u'name'+author_guid
        author.domain = u'test'
        self._db.add_author(author)
        self._author = author

    def _add_post(self, title, content, _domain=u'Microblog'):
        post = Post()
        post.author = self._author.author_full_name
        post.author_guid = self._author.author_guid
        post.content = content
        post.title = title
        post.domain = _domain
        post.post_id = title
        post.guid = title
        self._db.addPost(post)
        self._posts.append(post)

    def _get_posts_val(self): #return the vectors for all the words in the added posts
        vals = {}
        for post in self._posts:
            for word in post.content.split():
                if word in self._words.keys():
                    vals[word] = self._words[word]
        return vals.values()

    def _calc_mean(self, vectors):
        vectors = self._get_posts_val()
        if len(vectors) == 0:
            return (0,)*300
        ziped_vec = zip(*vectors)
        result = map(eval('np.mean'), ziped_vec)
        return tuple(result)

    def _calc_min(self, vectors):
        vectors = self._get_posts_val()
        if len(vectors) == 0:
            return (0,) * 300
        ziped_vec = zip(*vectors)
        result = map(eval('min'), ziped_vec)
        return tuple(result)

    def _calc_max(self, vectors):
        vectors = self._get_posts_val()
        if len(vectors) == 0:
            return (0,) * 300
        ziped_vec = zip(*vectors)
        result = map(eval('max'), ziped_vec)
        return tuple(result)

    def _calc_results(self):
        vectors = self._words_vectors
        results = {}
        results[u'min'] = self._calc_min(vectors)
        results[u'max'] = self._calc_max(vectors)
        results[u'np.mean'] = self._calc_mean(vectors)
        return results

    def _add_target_article(self, post_id, title, description, author_guid):
        target_article = Target_Article()
        target_article.author_guid=author_guid
        target_article.post_id = post_id
        target_article.title = title
        target_article.description = description
        target_article.keywords = 'temp, lala, fafa'
        self._db.add_target_articles([target_article])

    def _add_target_article_item(self, post_id, type, content, author_guid):
        article_item = Target_Article_Item()
        article_item.post_id = post_id
        article_item.type = type
        article_item.item_number = 3
        article_item.content = content
        article_item.author_guid = author_guid
        self._db.addPosts([article_item])

    def _add_claim_tweet_connection(self, claim_id, post_id):
        connection = Claim_Tweet_Connection()
        connection.claim_id = claim_id
        connection.post_id = post_id
        self._db.add_claim_connections([connection])
        pass