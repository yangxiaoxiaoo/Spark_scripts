import sys
import pickle
from pyspark import SparkContext
reload(sys)
sys.setdefaultencoding('utf-8')
sc = SparkContext(appName = "word_score_count")
infile = sc.textFile("hdfs:///user/xiaofeng/scored_lines.txt")
outfile = "hdfs:///user/xiaofeng/word_score.txt"

def parse(line):
    #take a line and turn it into a tuple of score, list of words
    e = line.split(';;')
    return int(e[1]), e[2].split(' ')




def score_word(pair):
    print pair
    word_score_list = list()
    score = pair[0]
    for word in pair[1]:
        word_score_list.append((word, (score, 1)))
    return word_score_list


def line_to_word():
    #produce tuples of (word, normalized score)
    word_score_dict= dict()

    word_to_score = infile.map(parse)\
        .flatMap(score_word)\
        .reduce(lambda p1, p2: (p1[0] + p2[0], p1[1]+p2[1]))\
        .map(lambda pair: (pair[0], pair[1][0]/pair[1][1]))

    word_to_score_tuples = word_to_score.collect()
    for pair in word_to_score_tuples:
        word_score_dict[pair[0]] = pair[1]
    return word_score_dict


def word_to_userscore(dict):
    shared_dict = sc.broadcast(dict)
    uid_to_score_dict = dict()

    def line_to_score(line):
        e = line.split(';;')
        uid = e[0]
        list = e[2].split(' ')
        score = 0
        count = 0
        for word in list:
            score += shared_dict[word]
            count += 1
            return uid, (score, count)

    uid_to_score = infile.map(line_to_score)\
        .reduce(lambda p1, p2: (p1[0] + p2[0], p1[1] + p2[1]))\
        .map(lambda pair: (pair[0], pair[1][0]/pair[1][1]))
    uid_to_score_tuples = uid_to_score.collect()
    for pair in uid_to_score_tuples:
        uid_to_score_dict[pair[0]] = pair[1]
    return uid_to_score_dict

if __name__ == "__main__":
    dict = line_to_word()
    user_score = word_to_userscore(dict)
    outfile = open("/home/xiaofeng/weibo_project/wumao/uid_sentiment_score.p",'wb')
    pickle.dump(user_score, outfile)
