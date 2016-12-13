import  sys
import copy
import math

def update_translate_map():
    '''
    translate_map['vhigh'] = 3.0
    translate_map['high'] = 2.0
    translate_map['med'] = 1.0
    translate_map['low'] = 0.0
    translate_map['2'] = 1.0
    translate_map['3'] = 2.0
    translate_map['4'] = 3.0
    translate_map['5more'] = 4.0
    translate_map['more'] = 5.0
    translate_map['small'] = 0.0
    translate_map['big'] = 2.0

    attributes[0]['vhigh'] = 1.0
    attributes[0]['high'] = 1.0/3 * 2
    attributes[0]['med'] = 1.0/3 * 1
    attributes[0]['low'] = 0.0
    attributes[1]['vhigh'] = 1.0
    attributes[1]['high'] = 1.0/3 * 2
    attributes[1]['med'] = 1.0/3 * 1
    attributes[1]['low'] = 0.0
    attributes[2]['5more'] = 1.0
    attributes[2]['4'] = 1.0/3 * 2
    attributes[2]['3'] = 1.0/3 * 1
    attributes[2]['2'] = 0.0
    attributes[3]['more'] = 1.0
    attributes[3]['4'] = 0.5
    attributes[3]['2'] = 0.0
    attributes[4]['big'] = 1.0
    attributes[4]['med'] = 0.5
    attributes[4]['small'] = 0.0
    attributes[5]['high'] = 1.0
    attributes[5]['med'] = 0.5
    attributes[5]['low'] = 0.0
    '''
    attr_value[0]['vhigh'] = 3.0
    attr_value[0]['high'] = 2.0
    attr_value[0]['med'] = 1.0
    attr_value[0]['low'] = 0.0
    attr_value[1]['vhigh'] = 3.0
    attr_value[1]['high'] = 2.0
    attr_value[1]['med'] = 1.0
    attr_value[1]['low'] = 0.0
    attr_value[2]['5more'] = 3.0
    attr_value[2]['4'] = 2.0
    attr_value[2]['3'] = 1.0
    attr_value[2]['2'] = 0.0
    attr_value[3]['more'] = 2.0
    attr_value[3]['4'] = 1.0
    attr_value[3]['2'] = 0.0
    attr_value[4]['big'] = 2.0
    attr_value[4]['med'] = 1.0
    attr_value[4]['small'] = 0.0
    attr_value[5]['high'] = 2.0
    attr_value[5]['med'] = 1.0
    attr_value[5]['low'] = 0.0


def get_small_euclidean_distance(line):
    min_distance = sys.maxint
    min_distance_cluster = 0
    for key,value in k_centroid.items():
        sum = 0
        for i in range(len(value)):
            sum += ((line[i] - value[i]) ** 2)
        sum = math.sqrt(sum)
        #print sum
        if sum < min_distance:
            min_distance = sum
            min_distance_cluster = key
    return min_distance_cluster
# check again tmr

def translate_num(line,ran_num):
    for i in range(ran_num):
        line[i] = attr_value[i][line[i]]
        #line[i] = translate_map[i][line[i]]
    return line


def get_cars_inf(input_file):
    cars_file = open(input_file,'r')
    count = 1
    for line in cars_file:
        line = line.strip('\n\r')
        line = line.split(',')
        line.append(0)
        cars_dict[count] = line
        count += 1
    #print cars_dict
    #print count

def get_initailPoints_file(initialPoints_file,k_clusters):
    k_cent_file = open(initialPoints_file, 'r')
    k_count = 1
    for line in k_cent_file:
        line = line.strip('\n\r')
        line = line.split(',')
        line  = translate_num(line, len(line)- 1)
        k_centroid[k_count] = line[:6]
        k_count += 1
    #print k_centroid
def inital_store_dict(k_clusters):
    for i in range(k_clusters):
        new_k_centroid_count[i + 1] = [0.0,0.0,0.0,0.0,0.0,0.0,0.0]
        class_count.append([0,0,0,0])

def main(input_file,initialPoints_file,k_clusters,itera_num):
    update_translate_map()
    get_cars_inf(input_file)
    get_initailPoints_file(initialPoints_file,k_clusters)
    inital_store_dict(k_clusters)
    final_list=[]
    for i in range(k_clusters):
        final_list.append([])
    while itera_num > 0:
        #print itera_num
        for i in range(k_clusters):
            new_k_centroid_count[i + 1] = [0.0,0.0,0.0,0.0,0.0,0.0,0]
        for key,value in cars_dict.items():
            #print "this is value"
            line = copy.deepcopy(value[:6])
            #print line
            line = translate_num(line, len(line))
            cluster_ID = get_small_euclidean_distance(line)
            line.append(1.0)
            for x in range(len(line)):
                new_k_centroid_count[cluster_ID][x] += line[x]
            value[len(value) -1] = cluster_ID
            '''
            if value[len(value) - 2] == 'unacc':
                class_count[cluster_ID -1][0] += 1
            elif value[len(value) - 2] == 'acc':
                class_count[cluster_ID -1][1] += 1
            elif value[len(value) - 2] == 'good':
                class_count[cluster_ID -1][2] += 1
            elif value[len(value) - 2] == 'vgood':
                class_count[cluster_ID -1][3] += 1
            '''
            if itera_num == 1:
                #print "this is cluster_ID "
                #print cluster_ID
                final_list[cluster_ID - 1].append(value[:len(value) - 1])
                if value[len(value) - 2] == 'unacc':
                    class_count[cluster_ID -1][0] += 1
                elif value[len(value) - 2] == 'acc':
                    class_count[cluster_ID -1][1] += 1
                elif value[len(value) - 2] == 'good':
                    class_count[cluster_ID -1][2] += 1
                elif value[len(value) - 2] == 'vgood':
                    class_count[cluster_ID -1][3] += 1

        for key,value in new_k_centroid_count.items():
            for i in range(len(value) -1):
                value[i] =  value[i] / value[len(value) -1]


        for key,value in k_centroid.items():
            for key1, value1 in new_k_centroid_count.items():
                if key == key1:
                    k_centroid[key] = new_k_centroid_count[key1][:len(value1) -1]
        itera_num = itera_num - 1
    count = 0
    # get the cluster name list
    error_number = 0
    cluster_name_list=[]

    for item in class_count:
        max = 0
        sum = 0
        index = 0
        for i in range(len(item)):
            sum += item[i]
            if item[i] >= max:
                max = item[i]
                index = i
        error_number += sum - max;
        if index == 0:
           cluster_name_list.append('unacc')
        elif index == 1:
            cluster_name_list.append('acc')
        elif index == 2:
            cluster_name_list.append('good')
        else:
            cluster_name_list.append('vgood')

    for i in range(len(final_list)):
        output.write("cluster: "+ cluster_name_list[i] + "\n")
        #print "cluster name", cluster_name_list[i]
        for data in final_list[i]:
             #print data
             output.write(str(data) + "\n")
        output.write("\n")
        output.write("\n")
    output.write("Number of points wrongly assigned:" + "\n")
    output.write(str(error_number))
    '''
    print class_count
    print "below is the cluster name list"
    print cluster_name_list
    print error_number
    '''
    #for key,value in cars_dict.items():
    #need to output result

input_file = sys.argv[1]
initialPoints_file = sys.argv[2]
k_clusters = int(sys.argv[3])
itera_num = int(int(sys.argv[4]))
'''
input_file = 'input_car'
k_clusters = 4
itera_num = 10
initialPoints_file = 'initialPoints'
'''
cars_dict = {}
k_centroid = {}
new_k_centroid_count = {}
class_count = []
attr_value =[{},{},{},{},{},{}]
output_file_name = 'mao_dilin_ouput.txt'
output = open(output_file_name, 'w')

main(input_file,initialPoints_file,k_clusters,itera_num)
output.close()
