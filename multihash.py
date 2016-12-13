import sys
import itertools
import json
#initiate
# record index
single_item_candidate ={}
pre_buckets_one ={}
pre_buckets_two ={}

#hashfunction one index + index
def hashfunction_one(itemset,bucket_size):
    itemset_len = len(itemset)
    sum = 0
    for i in range(itemset_len):
        sum += int(single_item_candidate[itemset[i]][0])
    hash_one_bucket_num = sum % bucket_size
    return hash_one_bucket_num

#hashfunction tow index*index
def hashfunction_two(itemset,bucket_size):
    itemset_len = len(itemset)
    prod = 1
    for i in range(itemset_len):
        prod *= int(single_item_candidate[itemset[i]][0])
    hash_two_bucket_num = prod % bucket_size
    return hash_two_bucket_num

# map each pair to bucket
def maping_pair_to_bucket(itemset,hash_one_buckets,hash_two_buckets):
        hash_one_bucket_num = hashfunction_one(itemset,bucket_size)
        hash_one_buckets[hash_one_bucket_num] += 1
        hash_two_bucket_num = hashfunction_two(itemset,bucket_size)
        hash_two_buckets[hash_two_bucket_num] += 1

def get_bitmap(bucket, support):
    bitmap = 0
    for k, v in bucket.items():
        if v >= support:
            bitmap += 1 << k
    return bitmap

def mulithash_frequent_itemsets(filename,fre_single_item,bit_map_one,bit_map_two,pre_buckets_one,pre_buckets_two):
    fre_itemsets = fre_single_item
    bit_map_one = bit_map_one
    bit_map_two = bit_map_two
    frequent_itemset_size = int(len(fre_itemsets[0]))

    while len(fre_itemsets)!= 0:
        fre_itemsets_candidate ={}
        input = open(filename,"rU")
        next_frequent_itemset_size = len(fre_itemsets[0]) + 1
        hash_next_itemset = next_frequent_itemset_size + 1
        hash_one_dict ={}
        hash_two_dict ={}
        for i in range(bucket_size):
             hash_one_dict[i] = 0
             hash_two_dict[i] = 0
        for line in input:
            line = line.lower()
            basket = line.strip('\n').split(',')
            basket.sort()

            if len(basket) >= hash_next_itemset:
                basket_itemsets = itertools.combinations(basket, hash_next_itemset)
                for itemset in basket_itemsets:
                    maping_pair_to_bucket(itemset,hash_one_dict,hash_two_dict)

            if len(basket) >= next_frequent_itemset_size:
                basket_itemsets = itertools.combinations(basket, next_frequent_itemset_size)
                for itemset in basket_itemsets:
                    hash_one_bucket_num = hashfunction_one(itemset,bucket_size)
                    if 1 << hash_one_bucket_num & bit_map_one == 0:
                        continue
                    # test if it is true in hashfuntion two buckets
                    hash_two_bucket_num = hashfunction_two(itemset,bucket_size)
                    if 1 << hash_two_bucket_num & bit_map_two == 0:
                        continue

                    #check k-1 frequent itemset
                    if len(itemset) == 2 :
                        if itemset[0] not in fre_itemsets or itemset[1] not in fre_itemsets:
                           continue
                    else:
                        subsets = itertools.combinations(itemset,len(fre_itemsets[0]))
                        i = 0
                        for subset in subsets:
                            if subset not in fre_itemsets:
                                break
                            i += 1
                        if i != next_frequent_itemset_size:
                            continue
                    #if the program can be here it means it is a candidate
                    if fre_itemsets_candidate.has_key(itemset):
                        fre_itemsets_candidate[itemset] +=1
                    else:
                        fre_itemsets_candidate[itemset] = 1

        fre_itemsets = []
        pri_fre_itemsets =[]
        for k,v in fre_itemsets_candidate.items():
            if v >= support:
               fre_itemsets.append(k)
               pri_fre_itemsets.append(list(k))
        fre_itemsets.sort()
        pri_fre_itemsets.sort()

        if len(fre_itemsets)> 0:
            output.write("\n")
            output.write(json.dumps(pre_buckets_one).replace('"',"") + "\n")
            output.write(json.dumps(pre_buckets_two).replace('"',"") + "\n")
            output.write(json.dumps(pri_fre_itemsets).replace('"',"'") + "\n")

        pre_buckets_one = hash_one_dict
        pre_buckets_two  = hash_two_dict
        #hange the bit map
        bit_map_one = get_bitmap(hash_one_dict, support)
        bit_map_two = get_bitmap(hash_two_dict, support)
        input.close()

def mulithash_one_pass(filename,support,bucket_size):
    input = open(filename,"rU")
    # get each basket
    buckets_one ={}
    buckets_two = {}
    index = 1
    #initial bucketsone and two
    for i in range(bucket_size):
        buckets_one[i] = 0
        buckets_two[i] = 0
    for line in input:
        #preprocoesss each basket,lowercase,sort,to a list
        line = line.lower()
        basket =line.strip('\n').split(',')
        basket.sort()
        #print basket
        #count the number of single item in each basket
        for single_item in basket:
            if(single_item_candidate.has_key(single_item)):
                single_item_candidate[single_item] = [ single_item_candidate[single_item][0], single_item_candidate[single_item][1] + 1]
            else:
                content =[index,1]
                index += 1
                single_item_candidate[single_item] = content
        #test
        #print "this is test the itertool"
        # first Two hashfunctions for the pairs
        if len(basket) >=2:
            basket_itemsets = itertools.combinations(basket, 2)
            for itemset in basket_itemsets:
                #print "here"
                #print itemset
                maping_pair_to_bucket(itemset,buckets_one,buckets_two)
            pre_buckets_one = buckets_one
            pre_buckets_two  = buckets_two
    input.close()
    #print out the frequent single itemset
    fre_single_item =[]
    for k,v in single_item_candidate.items():
        if(v[1] >= support):
            fre_single_item.append(k)
    fre_single_item.sort()
    # generate two bitmap
    bit_map_one = get_bitmap(buckets_one, support)
    bit_map_two = get_bitmap(buckets_two, support)
    if len(fre_single_item)> 0:
        str_json = json.dumps(fre_single_item)
        str_json = str_json.replace('"',"'")
        output.write(str_json + "\n")
        #print fre_single_item
        mulithash_frequent_itemsets(filename,fre_single_item,bit_map_one,bit_map_two,pre_buckets_one,pre_buckets_two)

#below is the main start.
#print "this is a test"
output_file_name = 'output_multihash.txt'
output = open(output_file_name, 'w')
filename = sys.argv[1]
support = int(sys.argv[2])
bucket_size = int(sys.argv[3])
#filename = 'input.txt'
#support = 4
#bucket_size = 5
mulithash_one_pass(filename, support, bucket_size)
output.close()