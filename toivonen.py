import sys
import random
import itertools
import json

#initiate  some basic element
def go_apriori(sample_basket_lists,sample_support):
    rst =[]
    all_fre_itemsets = []
    negative_border =[]
    do_while_flag = 0
    fre_itemsets =[]
    while do_while_flag == 0 or (do_while_flag != 0 and len(fre_itemsets) != 0):
        do_while_flag += 1
        candidate_itemsets_dict ={}
        if do_while_flag == 1:
            candidate_fre_itemsets_size = 1
        else:
            candidate_fre_itemsets_size = len(fre_itemsets[0]) + 1

        for sample_basket in sample_basket_lists:
            #preprocoesss each basket,lowercase,sort,to a list
            if len(sample_basket) >= candidate_fre_itemsets_size:
                if candidate_fre_itemsets_size == 1:
                    for single_item in sample_basket:
                        single_item = tuple(single_item)
                        if candidate_itemsets_dict.has_key(single_item):
                            candidate_itemsets_dict[single_item] = candidate_itemsets_dict[single_item] + 1
                        else:
                            candidate_itemsets_dict[single_item] =  1
                else:
                    sample_basket_itemsets =  itertools.combinations(sample_basket, candidate_fre_itemsets_size)
                    for itemset in sample_basket_itemsets:
                        subsets= itertools.combinations(itemset, candidate_fre_itemsets_size -1)
                        i = 0
                        for subset in subsets:
                            if subset not in fre_itemsets:
                                break
                            i +=1
                        if i != candidate_fre_itemsets_size:
                            continue
                        if candidate_itemsets_dict.has_key(itemset):
                            candidate_itemsets_dict[itemset] += 1
                        else:
                            candidate_itemsets_dict[itemset] = 1
        fre_itemsets = []
        for k, v in candidate_itemsets_dict.items():
            if v >= sample_support:
               fre_itemsets.append(k)
               all_fre_itemsets.append(k)
            else:
                negative_border.append(k)
    all_fre_itemsets.sort()
    #print all_fre_itemsets
    #print fre_itemsets
    negative_border.sort()
    #print negative_border
    rst.append(all_fre_itemsets)
    rst.append(negative_border)
    return rst
#### Aprioir end ####################################################################

def scan_whole_dataset(filename,support,rst):
    answer =[]
    final_fre_itemset =[]
    answer.append(False)
    answer.append(final_fre_itemset)
    #print "this is answer"
    #print answer
    input = open(filename,"rU")
    all_fre_itemsets_list = rst[0]
    #print all_fre_itemsets_list
    negative_border_list= rst[1]
    #print negative_border_list
    negative_border_dict ={}
    fre_itemsets_dict={}
    for line in input:
        line = line.lower()
        basket =line.strip('\n').split(',')
        basket.sort()
        length = len(basket)
        for i in range(1,length+1):
            basket_itemsets =  itertools.combinations(basket, i)
            for itemset in basket_itemsets:
                if fre_itemsets_dict.has_key(itemset):
                    fre_itemsets_dict[itemset] += 1
                else:
                    if itemset in all_fre_itemsets_list:
                        fre_itemsets_dict[itemset] = 1
                if itemset in negative_border_list:
                    if negative_border_dict.has_key(itemset):
                        if negative_border_dict[itemset] >= (support - 1):
                           return answer
                        else:
                            negative_border_dict[itemset] += 1
                    else:
                        negative_border_dict[itemset] = 1
    answer[0] = True
    for k,v in fre_itemsets_dict.items():
        if v >= support:
            final_fre_itemset.append(k)
    final_fre_itemset.sort(lambda x,y: cmp(len(x), len(y)))
    input.close()
    return answer

def get_sample(filename,sample_percentage):
    sample_basket_lists =[]
    input = open(filename,"rU")
    for line in input:
        if random.random() >= sample_percentage:
            line = line.lower()
            basket =line.strip('\n').split(',')
            basket.sort()
            sample_basket_lists.append(basket)
    input.close()
    return sample_basket_lists


def go_toivonen(filename,sample_support,sample_percentage,support):
    flag = False
    num_of_iteration = 0
    while flag == False:
        num_of_iteration +=1
        sample_basket_lists= get_sample(filename,sample_percentage)
        rst = go_apriori(sample_basket_lists,sample_support)
        answer = scan_whole_dataset(filename,support,rst)
        flag = answer[0]
        if flag == True:
            output.write(str(num_of_iteration) + "\n")
            output.write(str(sample_percentage) + "\n")
            #print answer[1]
            length = len(answer[1])
            if length!= 0:
                #print answer[1][length-1]
                largest_itemset_length = len(answer[1][length-1])
                #print largest_itemset_length
                if largest_itemset_length >= 1:
                    i = 1
                    while i <= largest_itemset_length:
                        tmp = []
                        for element in answer[1]:
                            element = list(element)
                            if len(element) == i:
                                tmp.append(element)
                        tmp.sort()
                        if i == 1:
                            output.write(json.dumps(tmp).replace('"',"'") + "\n")
                        else:
                            output.write("\n")
                            output.write(json.dumps(tmp).replace('"',"'") + "\n")
                        i += 1

#below is the main function
filename = sys.argv[1]
support = int(sys.argv[2])
output_file_name = 'output_toivonen.txt'
output = open(output_file_name, 'w')
#filename = 'input1.txt'
#support = 20
sample_percentage = 0.4
#lower the threshold slightly
sample_support = support * sample_percentage * 0.9
go_toivonen(filename,sample_support,sample_percentage,support)
