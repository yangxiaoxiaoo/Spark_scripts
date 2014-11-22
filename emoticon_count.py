__author__ = 'xiaofeng'
#modified Nov 21 for preprocessing a score for each line
#output a file with each line as "uid;;score;;content_seperated"
import sys
import re
from collections import Counter
import pickle
#from pyspark import SparkContext
reload(sys)
sys.setdefaultencoding('utf-8')

token_list_length = 100
#give a sentiment value to the 100 most frequent users

def stat_token(filename):
    #all file -> Top words list of tuples
    #Input(simplified) can be "all_text_inline.txt".
    words = list()
    fp = open(filename, 'r')
    for line in fp:
        token_list= line.split(' ')
        words += token_list
    Top_result = Counter(words).most_common(token_list_length)
    all_result = Counter(words).most_common()
    sense_result = [item for item in all_result if item not in Top_result]
    print len(sense_result)
    print "of:"
    print len(all_result)
    return sense_result


def calc_line(line, pos_set, neg_set):
    #line -> sentiment score
    regEmo= r'\[.*?]'
    result = 0
    if '[' in line:
        try:
            list_emo = re.findall(regEmo, line.split('\t')[1])
            for emo in list_emo:
                if emo in pos_set:
                    result +=1
                if emo in neg_set:
                    result -=1
        except Exception as line_not_parsed:
            pass
    return result

def calc_score(Token_list, pos_set, neg_set):
    #Top token tuples -> a dictionary from token to sentiment score
    score_dict = dict()
    for token in Token_list:
        score_dict[token[0]] = 0
    data_file = "all_text_tokenized.txt"
    for line in data_file:
        line_list = line.split(' ')
        username = line_list[0]
        for token in Token_list:
            if token[0] in line_list:
                score_dict[token[0]] += calc_line(line, pos_set, neg_set)

    print len(score_dict)
    return score_dict

def accumulate_user_score(Token_to_score, infile, reffile):
    #accumulate_user_score(Top_token_score, "../all_text_tokenized.txt")
    #dict, infile -> dict from user to score
    line_to_uid = dict()
    fp0 = open(reffile, 'r')
    line_count_inf = 0
    line_count_ref = 0
    for line in fp0:
        uid = line.split('\t')[0]
        line_to_uid[line_count_ref] = uid
        line_count_ref += 1

    result = dict()
    fp = open(infile, 'r')
    for line in fp:
        try:
            uid = line_to_uid[line_count_inf]
            content_list = line.split(' ')
            for token in Token_to_score:
                if token in content_list:
                    if uid in result.keys():
                        result[uid] += Token_to_score[token]
                    else:
                        result[uid] = Token_to_score[token]
        except Exception as line_format:
            pass
        line_count_inf += 1
    print len(result)
    return result


if __name__ == "__main__":
#    sc = SparkContext(appName = "CountEmoticons")
    regEmo= r'\[.*?]'

    pos_set = set()
    neg_set = set()
    pos_file = open("../positive.txt")
    for line in pos_file:
        try:
            emoticon = line.split('\t')[0]
            sign = line.split('\t')[1]
            if '+' in sign:
              #  print '+'
                pos_set.add(emoticon)
            if '-' in sign:
              #  print '-'
                neg_set.add(emoticon)
        except Exception as not_a_marked_value:
            pass
    print 'positive emoticon'
    print len(pos_set)
    print 'negetive emoticon'
    print len(neg_set)

    Filtered_result = stat_token("../all_text_tokenized.txt")
    print "filtered result calculated"
  #  Top_token_score = calc_score(Filtered_result, pos_set, neg_set)
  #  print "token score calculated"
  #  User_score = accumulate_user_score(Top_token_score, "../all_text_tokenized.txt", "../all_text.txt")
  #  print "user score calculated"
  #  pickle.dump(User_score, open("../User_to_score_dict", 'w'))
  #  for key, value in User_score.iteritems():
  #      fout = open("../User_to_score.txt", 'a')
  #      fout.write(str(key) + "::" + str(value) + '\n')
    data_file = open("../all_text_tokenized.txt", 'r')
    line_to_score = dict()
    line_num = 0
    for line in data_file:
        line_to_score[line_num] = calc_line(line, pos_set, neg_set)
        line_num += 1
    data_file.close()

    print "data_file_done"

    reference_file = "../all_text.txt"
    fp0 = open(reference_file, 'r')
    line_count_ref = 0
    line_to_uid = dict()
    for line in fp0:
        uid = line.split('\t')[0]
        line_to_uid[line_count_ref] = uid
        line_count_ref += 1
    fp0.close()

    print "ref_file_done"

    data_file1 = open("../all_text_tokenized.txt", 'r')
    outfile = open("scored_lines.txt", 'a')
    line_num = 0
    for line in data_file1:
        outfile.write(str(line_to_uid[line_num]) + ";;"+ str(line_to_score[line_num]) + ";;" + line)
        line_num += 1
    data_file1.close()
