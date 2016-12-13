import sys
import math
def readInputFile(rate_file):
    for line in rate_file:
        line = line.strip('\n')
        columns = line.split("\t")
        user_item_dict.setdefault(str(columns[0]),{})
        user_item_dict[str(columns[0])][str(columns[2])]=float(columns[1])
    return user_item_dict

def pearson_correlation(user1,user2):
    #return the similarity
    same_movie_count =0
    user1_moive_rate = user_item_dict[user1]
    user2_moive_rate = user_item_dict[user2]
    #find the shared movie rate
    user1_rate = []
    user2_rate = []
    for user1_key in user1_moive_rate:
        for user2_key in user2_moive_rate:
            if user1_key == user2_key:
                same_movie_count +=1;
                user1_rate.append(user1_moive_rate[user1_key])
                user2_rate.append(user2_moive_rate[user2_key])
    if same_movie_count == 0:
        return 0.0
    else:
        user1_rate_ave = 0
        dot_product = 0
        sum1 = 0
        sum2 = 0
        user2_rate_ave = 0
        for rate in user1_rate:
            sum1 += rate
        for rate in user2_rate:
            sum2 += rate
        user1_rate_ave = sum1 / len(user1_rate)
        user2_rate_ave = sum2 / len(user2_rate)
        if user2_corate_average.has_key(user2) == False:
            user2_corate_average[user2] = user2_rate_ave
        for i in range(len(user1_rate)):
            dot_product += (user1_rate[i] - user1_rate_ave )* (user2_rate[i] - user2_rate_ave)

        user1_vertor = 0
        user2_vertor = 0
        for x in user1_rate:
            user1_vertor += (x - user1_rate_ave) * (x - user1_rate_ave)
        user1_vertor = math.sqrt(user1_vertor)
        for x in user2_rate:
            user2_vertor += (x - user2_rate_ave) * (x - user2_rate_ave)
        user2_vertor = math.sqrt(user2_vertor)
        similarity = dot_product / (user2_vertor * user1_vertor)
        return similarity

def K_nearest_neighbors(user1,item,neighbor_num):
    k_nearest =[]
    for key in user_item_dict:
        if key != user1 and user_item_dict[key].has_key(item) :
            k_nearest.append((key,pearson_correlation(user1,key)))
    k_nearest=sorted(k_nearest,key=lambda x:(x[1],x[0]),reverse=True)
    return k_nearest[0:neighbor_num]

def Predict(user1, item, neighbor_list):
    if user_item_dict[user1].has_key(item):
        return user_item_dict[user1][item]
    else:
        #conunt user1 all average:
        user1_all_average = 0
        count = 0
        user1_all_sum = 0
        user1_moive_rate = user_item_dict[user1]
        for user1_key in user1_moive_rate:
            count += 1
            user1_all_sum += user1_moive_rate[user1_key]


        user1_all_average = user1_all_sum / count
        neighbor_rate_weigth_product = 0
        neighbor_weight = 0

        for k in neighbor_list:
            if user_item_dict[k[0]].has_key(item):
                neighbor_rate_weigth_product += (user_item_dict[k[0]][item] - user2_corate_average[k[0]]) * k[1]
                neighbor_weight += math.fabs(k[1])

        rst = user1_all_average + (neighbor_rate_weigth_product / neighbor_weight)
        return rst


def main(filename,user1,item,neighbor_num):
    rate_file = open(filename, "r")
    #read the input file
    user_item_dict = readInputFile(rate_file)
    #print(pearson_correlation("Kluver","k279"))
    #print(pearson_correlation("Kluver", "Connor M"))
    neighbor_list = K_nearest_neighbors(user1,item,neighbor_num)
    for k in neighbor_list:
        line = str(k[0])  + "  " + str(k[1])
        print line
    print Predict(user1, item, neighbor_list)

#filename = 'ratings-dataset.tsv'
#user1 = 'Kluver'
#movie = 'The Fugitive'
#neighbor_num = 10
user_item_dict = {}
user2_corate_average= {}
filename = sys.argv[1]
user1 = sys.argv[2]
movie = sys.argv[3]
neighbor_num = int(sys.argv[4])
main(filename,user1,movie,neighbor_num)



