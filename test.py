import collections
import pickle

words = list()
for i in range(1, 10):
    words.append(i)
words.append(7)
c = collections.Counter(words).most_common(2)
print c
for item in c:
    print item[0]
    print item[1]
#pickle.dump(c, open("text_pickle", 'w'))

pos_file = open("../positive.txt")
for line in pos_file:
 #   print line.split('\t')[1]
    try:
        emoticon = line.split('\t')[0]
        sign = line.split('\t')[1]
        if '+' in sign:
            print '+'
        if '-' in sign:
            print '-'
    except Exception as not_a_marked_value:
        pass

fp = open("../all_text_tokenized.txt", 'r')
for line in fp:
    try:
        content_list = line.split(' ')
        for cont in content_list:
            print cont
    except Exception as line_format:
        pass
