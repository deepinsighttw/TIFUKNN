import random
import numpy as np
import sys
import math
import csv
import gc
import psutil
import orjson

from loguru import logger

logger.add("out_{time}.log", backtrace=True, diagnose=True) 

activate_codes_num = -1
next_k_step = 1
training_chunk = 0
test_chunk = 1

def log_memory():
    logger.info(f'RAM memory used: {psutil.virtual_memory()[2]}%')

def add_history(data_history, training_key_set, output_size):
    sum_history = {}
    for key in training_key_set:
        sum_vector = np.zeros(output_size)
        count = 0
        for lst in data_history[key]:
            vec = np.zeros(output_size)
            for ele in lst:
                vec[ele] = 1
            if vec[-2] == 1 or vec[-1] == 1:
                continue
            sum_vector += vec
            count += 1
        sum_vector = sum_vector / count
        sum_history[key] = sum_vector
    return sum_history


def temporal_decay_add_history(data_set, key_set, output_size, within_decay_rate):
    sum_history = {}
    for key in key_set:
        vec_list = data_set[key]
        num_vec = len(vec_list) - 2
        his_list = np.zeros(output_size)
        for idx in range(1, num_vec + 1):
            his_vec = np.zeros(output_size)
            decayed_val = np.power(within_decay_rate, num_vec - idx)
            for ele in vec_list[idx]:
                his_vec[ele] = decayed_val
            his_list += his_vec
        sum_history[key] = his_list / num_vec
        # sum_history[key] = np.multiply(his_list / num_vec, IDF)

    return sum_history


def KNN(query_set, target_set, k):
    # TODO: check why copy again?
    history_mat = []
    for key in target_set.keys():
        history_mat.append(target_set[key])
    test_mat = []
    for key in query_set.keys():
        test_mat.append(query_set[key])
    logger.info('Finding k nearest neighbors...')
    nbrs = NearestNeighbors(n_neighbors=k, algorithm="brute").fit(history_mat)
    distances, indices = nbrs.kneighbors(test_mat)
    logger.info('Finish KNN search.' )
    return indices, distances


def weighted_aggragate_outputs(
    data_chunk, training_key_set, index, distance, output_size
):
    output_vectors = []
    key_set = training_key_set
    for index_list_id in range(len(index)):
        outputs = []
        for vec_idx in range(1, next_k_step + 1):

            target_vec_list = []
            weight_list = []
            for id in range(len(index[index_list_id])):
                dis = distance[index_list_id][id]
                if dis == 0:
                    weight_list.append(0)
                else:
                    weight_list.append(1 / dis)
            new_weight = softmax(weight_list)
            for i in range(len(new_weight)):
                if new_weight[i] == 0:
                    new_weight[i] = 1
            vec = np.zeros(output_size)
            for id in range(len(index[index_list_id])):
                idx = index[index_list_id][id]
                target_list = data_chunk[test_chunk][key_set[idx]][vec_idx]
                for ele in target_list:
                    vec[ele] += new_weight[id]
            outputs.append(vec)
        output_vectors.append(outputs)
    return output_vectors


from sklearn.neighbors import NearestNeighbors


def KNN_history_record1(sum_history, output_size, k):
    history_mat = []
    for key in sum_history.keys():
        history_mat.append(sum_history[key])

    logger.info("Finding k nearest neighbors...")
    nbrs = NearestNeighbors(n_neighbors=k, algorithm="brute").fit(history_mat)
    distances, indices = nbrs.kneighbors(history_mat)
    KNN_history = {}
    key_set = list(sum_history)
    for id in range(len(key_set)):
        #    for idx_list in indices:
        idx_list = indices[id]
        NN_history = np.zeros(output_size)
        for idx in idx_list:
            NN_history += sum_history[key_set[idx]]
        NN_history = NN_history / k
        KNN_history[key_set[id]] = NN_history

    return KNN_history


def KNN_history_record2(query_set, sum_history, output_size, k):
    history_mat = []
    for key in sum_history.keys():
        history_mat.append(sum_history[key])
    test_mat = []
    for key in query_set.keys():
        test_mat.append(query_set[key])
    logger.info("Finding k nearest neighbors...")
    nbrs = NearestNeighbors(n_neighbors=k, algorithm="brute").fit(history_mat)
    distances, indices = nbrs.kneighbors(test_mat)
    KNN_history = {}
    key_set = list(query_set)
    training_key_set = list(sum_history)
    for id in range(len(key_set)):
        #    for idx_list in indices:
        idx_list = indices[id]
        NN_history = np.zeros(output_size)
        for idx in idx_list:
            NN_history += sum_history[training_key_set[idx]]
        NN_history = NN_history / k
        KNN_history[key_set[id]] = NN_history

    return KNN_history, indices


def group_history_list(his_list, group_size):
    grouped_vec_list = []
    if len(his_list) < group_size:
        # sum = np.zeros(len(his_list[0]))
        for j in range(len(his_list)):
            grouped_vec_list.append(his_list[j])

        return grouped_vec_list, len(his_list)
    else:
        est_num_vec_each_block = len(his_list) / group_size
        base_num_vec_each_block = int(np.floor(len(his_list) / group_size))
        residual = est_num_vec_each_block - base_num_vec_each_block

        num_vec_has_extra_vec = int(np.round(residual * group_size))

        if residual == 0:
            for i in range(group_size):
                if len(his_list) < 1:
                    logger.info("len(his_list)<1")
                sum = np.zeros(len(his_list[0]))
                for j in range(base_num_vec_each_block):
                    if i * base_num_vec_each_block + j >= len(his_list):
                        logger.info("i*num_vec_each_block+j")
                    sum += his_list[i * base_num_vec_each_block + j]
                grouped_vec_list.append(sum / base_num_vec_each_block)
        else:

            for i in range(group_size - num_vec_has_extra_vec):
                sum = np.zeros(len(his_list[0]))
                for j in range(base_num_vec_each_block):
                    if i * base_num_vec_each_block + j >= len(his_list):
                        logger.info("i*base_num_vec_each_block+j")
                    sum += his_list[i * base_num_vec_each_block + j]
                    last_idx = i * base_num_vec_each_block + j
                grouped_vec_list.append(sum / base_num_vec_each_block)

            est_num = int(np.ceil(est_num_vec_each_block))
            start_group_idx = group_size - num_vec_has_extra_vec
            if (
                len(his_list) - start_group_idx * base_num_vec_each_block
                >= est_num_vec_each_block
            ):
                for i in range(start_group_idx, group_size):
                    sum = np.zeros(len(his_list[0]))
                    for j in range(est_num):
                        # if residual+(i-1)*est_num_vec_each_block+j >= len(his_list):
                        #     logger.info('residual+(i-1)*num_vec_each_block+j')
                        #     logger.info('len(his_list)')
                        iidxx = last_idx + 1 + (i - start_group_idx) * est_num + j
                        if iidxx >= len(his_list) or iidxx < 0:
                            logger.info("last_idx + 1+(i-start_group_idx)*est_num+j")
                        sum += his_list[iidxx]
                    grouped_vec_list.append(sum / est_num)

        return grouped_vec_list, group_size


def get_sum_history(
    data_set, key_set, output_size, group_size
):
    sum_history = {}
    for key in key_set:
        vec_list = data_set[key]
        num_vec = len(vec_list) - 2
        his_list = []
        for idx in range(1, num_vec + 1):
            his_vec = np.zeros(output_size)
            for ele in vec_list[idx]:
                his_vec[ele] = 1
            his_list.append(his_vec)

        grouped_list, real_group_size = group_history_list(his_list, group_size)
        his_vec = np.zeros(output_size)
        for idx in range(real_group_size):
            if idx >= len(grouped_list):
                logger.info("idx: " + str(idx))
                logger.info("len(grouped_list): " + str(len(grouped_list)))
            his_vec += grouped_list[idx]
        sum_history[key] = his_vec / real_group_size
    return sum_history


def temporal_decay_sum_history(
    data_set, key_set, output_size, group_size, within_decay_rate, group_decay_rate
):
    sum_history = {}
    for key in key_set:
        vec_list = data_set[key]
        num_vec = len(vec_list) - 2
        his_list = []
        for idx in range(1, num_vec + 1):
            his_vec = np.zeros(output_size)
            decayed_val = np.power(within_decay_rate, num_vec - idx)
            for ele in vec_list[idx]:
                his_vec[ele] = decayed_val
            his_list.append(his_vec)

        grouped_list, real_group_size = group_history_list(his_list, group_size)
        his_vec = np.zeros(output_size)
        for idx in range(real_group_size):
            decayed_val = np.power(group_decay_rate, group_size - 1 - idx)
            if idx >= len(grouped_list):
                logger.info("idx: " + str(idx))
                logger.info("len(grouped_list): " + str(len(grouped_list)))
            his_vec += grouped_list[idx] * decayed_val
        sum_history[key] = his_vec / real_group_size
        # sum_history[key] = np.multiply(his_vec / real_group_size, IDF)
    return sum_history


def partition_the_data(data_chunk, key_set):
    filtered_key_set = []
    for key in key_set:
        if len(data_chunk[training_chunk][key]) <= 3:
            continue
        if len(data_chunk[test_chunk][key]) < 2 + next_k_step:
            continue
        filtered_key_set.append(key)

    training_key_set = filtered_key_set[0 : int(4 / 5 * len(filtered_key_set))]
    logger.info(len(training_key_set))
    test_key_set = filtered_key_set[int(4 / 5 * len(filtered_key_set)) :]
    return training_key_set, test_key_set


def partition_the_data_validate(data_chunk, key_set, next_k_step):
    filtered_key_set = []
    past_chunk = 0
    future_chunk = 1
    for key in key_set:
        # 訂單記錄至少一次 + start tmp + end tmp？
        # 論文寫 remove all the customers who have baskets less than 3 to ensure that temporal patterns exist in the past records
        if len(data_chunk[past_chunk][key]) <= 3:
            continue
        if len(data_chunk[future_chunk][key]) < 2 + next_k_step:
            continue
        filtered_key_set.append(key)

    training_key_set = filtered_key_set[0 : int(4 / 5 * len(filtered_key_set) * 0.9)]
    validation_key_set = filtered_key_set[
        int(4 / 5 * len(filtered_key_set) * 0.9) : int(4 / 5 * len(filtered_key_set))
    ]

    test_key_set = filtered_key_set[int(4 / 5 * len(filtered_key_set)) :]

    logger.info("Number of training instances: " + str(len(training_key_set)))
    logger.info("Number of validation instances: " + str(len(validation_key_set)))
    logger.info("Number of test instances: " + str(len(test_key_set)))

    return training_key_set, validation_key_set, test_key_set


# neighbors?
# def most_frequent_elements(data_chunk, index, training_key_set, output_size):
#     output_vectors = []

#     for vec_idx in range(1, next_k_step + 1):
#         vec = np.zeros(output_size)
#         for idx in index:
#             target_vec = data_chunk[test_chunk][training_key_set[idx]][vec_idx]
#             for ele in target_vec:
#                 vec[ele] += 1

#         output_vectors.append(vec)
#     return output_vectors

# PIF
# def predict_with_PIF(input_variable, output_size):
#     vec_list = input_variable
#     output_vectors = []
#     num_vec = len(vec_list) - 2
#     for idx in range(1, num_vec + 1):
#         vec = np.zeros(output_size)
#         for ele in vec_list[idx]:
#             his_vec[ele] += 1

#     for idx in range(1, num_vec + 1):
#         his_vec = np.zeros(output_size)
#         for ele in vec_list[idx]:
#             his_vec[ele] = 1
#         his_list.append(his_vec)


#     for idx in range(next_k_step):
#         output_vectors.append(vec)

#     return output_vectors


def predict_with_elements_in_input(sum_history, key):
    output_vectors = []

    for idx in range(next_k_step):
        vec = sum_history[key]
        output_vectors.append(vec)
    return output_vectors


def generate_dictionary_BA(files, attributes_list):
    # path = '../Minnemudac/'
    # files = ['Coborn_history_order.csv','Coborn_future_order.csv']
    # files = ['BA_history_order.csv', 'BA_future_order.csv']
    # attributes_list = ['item_id']

    # 目的應該為重新幫 item_id 等 attr 重新編號，從 0 開始
    dictionary_table = {}
    counter_table = {}
    for attr in attributes_list:
        dictionary = {}
        dictionary_table[attr] = dictionary
        counter_table[attr] = 0

    csv.field_size_limit(sys.maxsize)
    for filename in files:
        count = 0
        with open(filename, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=",", quotechar="|")
            for row in reader:
                if count == 0:
                    count += 1
                    continue
                # key = 'item_id'
                key = attributes_list[0]
                # row: user_id,item_id,txn_id
                if row[1] not in dictionary_table[key]:
                    dictionary_table[key][row[1]] = counter_table[key]
                    counter_table[key] = counter_table[key] + 1
                    count += 1

    logger.info(counter_table)

    total = 0
    for key in counter_table.keys():
        total = total + counter_table[key]

    logger.info("# dimensions of final vector: " + str(total) + " | " + str(count - 1))

    return dictionary_table, total, counter_table


def read_claim2vector_embedding_file_no_vector(files):
    # attributes_list = ['DRG', 'PROVCAT ', 'RVNU_CD', 'DIAG', 'PROC']
    attributes_list = ["item_id"]
    # path = '../Minnemudac/'
    logger.info("start dictionary generation...")
    dictionary_table, num_dim, counter_table = generate_dictionary_BA(
        files, attributes_list
    )
    logger.info("finish dictionary generation*****")
    usr_attr = "user_id"
    ord_attr = "txn_id"

    # dictionary_table, num_dim, counter_table = GDF.generate_dictionary(attributes_list)

    freq_max = 200
    ## all the follow three ways array. First index is patient, second index is the time step, third is the feature vector
    data_chunk = []
    day_gap_counter = []
    claims_counter = 0
    num_claim = 0
    code_freq_at_first_claim = np.zeros(num_dim + 2)
    tmp = [-1]

    for file_id in range(len(files)):

        count = 0
        data_chunk.append({})
        filename = files[file_id]
        with open(filename, "r") as csvfile:
            # gap_within_one_year = np.zeros(365)
            reader = csv.DictReader(csvfile)
            # last_pid_date 和 last_pid 應該是為了 debug 用的提前終止符號
            last_pid_date = "*"
            last_pid = "-1"
            last_days = -1
            # 2 more elements in the end for start and end states
            feature_vector = []
            for row in reader:
                cur_pid_date = row[usr_attr] + "_" + row[ord_attr]
                cur_pid = row[usr_attr]
                # cur_days = int(row[ord_attr])

                if cur_pid != last_pid:
                    # start state
                    # tmp = [-1]
                    data_chunk[file_id][cur_pid] = []
                    data_chunk[file_id][cur_pid].append(tmp)
                    num_claim = 0
                # else:
                #     if last_days != cur_days and last_days != -1 and file_id != 0 and file_id != 2:
                #
                #         gap_within_one_year[cur_days - last_days] = gap_within_one_year[cur_days - last_days] + 1

                # user_id: [[-1], [1st item vector], [2nd item vector], ..., [-1]]
                # 所以 user_id 的資料要放一起，並且要根據訂單次序排列
                if cur_pid_date not in last_pid_date:
                    if last_pid_date not in "*" and last_pid not in "-1":
                        sorted_feature_vector = np.sort(feature_vector)
                        data_chunk[file_id][last_pid].append(sorted_feature_vector)
                        if len(sorted_feature_vector) > 0:
                            count = count + 1
                            if count % 10000000 == 0:
                                logger.info(f"count: {count}")
                        # data_chunk[file_id][last_pid].append(feature_vector)
                    # 重置 feature_vector
                    feature_vector = []

                    claims_counter = 0
                if cur_pid != last_pid:
                    # end state
                    if last_pid not in "-1":

                        # tmp = [-1]
                        data_chunk[file_id][last_pid].append(tmp)
                # key = 'item_id'
                key = attributes_list[0]
                # item_id 對應的重新編碼
                within_idx = dictionary_table[key][row[key]]
                previous_idx = 0
                # 有點怪，照理來說會是 range(0)，所以 idx 應該都會是 encoded item id
                for j in range(attributes_list.index(key)):
                    previous_idx = previous_idx + counter_table[attributes_list[j]]
                idx = within_idx + previous_idx

                # set corresponding dimention to 1
                if idx not in feature_vector:
                    feature_vector.append(idx)

                last_pid_date = cur_pid_date
                last_pid = cur_pid
                # last_days = cur_days
                # future file
                if file_id == 1:
                    claims_counter = claims_counter + 1

            if last_pid_date not in "*" and last_pid not in "-1":
                data_chunk[file_id][last_pid].append(np.sort(feature_vector))

    return data_chunk, num_dim + 2, code_freq_at_first_claim


def get_precision_recall_Fscore(groundtruth, pred):
    a = groundtruth
    b = pred
    correct = 0
    truth = 0
    positive = 0

    for idx in range(len(a)):
        if a[idx] == 1:
            truth += 1
            if b[idx] == 1:
                correct += 1
        if b[idx] == 1:
            positive += 1

    flag = 0
    if 0 == positive:
        precision = 0
        flag = 1
        # logger.info('postivie is 0')
    else:
        precision = correct / positive
    if 0 == truth:
        recall = 0
        flag = 1
        # logger.info('recall is 0')
    else:
        recall = correct / truth

    if flag == 0 and precision + recall > 0:
        F = 2 * precision * recall / (precision + recall)
    else:
        F = 0
    return precision, recall, F, correct


def get_F_score(prediction, test_Y):
    jaccard_similarity = []
    prec = []
    rec = []

    count = 0
    for idx in range(len(test_Y)):
        pred = prediction[idx]
        T = 0
        P = 0
        correct = 0
        for id in range(len(pred)):
            if test_Y[idx][id] == 1:
                T = T + 1
                if pred[id] == 1:
                    correct = correct + 1
            if pred[id] == 1:
                P = P + 1

        if P == 0 or T == 0:
            continue
        precision = correct / P
        recall = correct / T
        prec.append(precision)
        rec.append(recall)
        if correct == 0:
            jaccard_similarity.append(0)
        else:
            jaccard_similarity.append(2 * precision * recall / (precision + recall))
        count = count + 1

    logger.info("average precision: " + str(np.mean(prec)))
    logger.info("average recall : " + str(np.mean(rec)))
    logger.info("average F score: " + str(np.mean(jaccard_similarity)))


def get_DCG(groundtruth, pred_rank_list, k):
    count = 0
    dcg = 0
    for pred in pred_rank_list:
        if count >= k:
            break
        if groundtruth[pred] == 1:
            dcg += (1) / math.log2(count + 1 + 1)
        count += 1

    return dcg


def get_NDCG1(groundtruth, pred_rank_list, k):
    count = 0
    dcg = 0
    for pred in pred_rank_list:
        if count >= k:
            break
        if groundtruth[pred] == 1:
            dcg += (1) / math.log2(count + 1 + 1)
        count += 1
    idcg = 0
    num_real_item = np.sum(groundtruth)
    num_item = int(num_real_item)
    for i in range(num_item):
        idcg += (1) / math.log2(i + 1 + 1)
    ndcg = dcg / idcg
    return ndcg


def get_HT(groundtruth, pred_rank_list, k):
    count = 0
    for pred in pred_rank_list:
        if count >= k:
            break
        if groundtruth[pred] == 1:
            return 1
        count += 1

    return 0


# input_size = 100
topk = 10


def softmax(x):
    """Compute softmax values for each sets of scores in x."""
    e_x = np.exp(x - np.max(x))
    return e_x / e_x.sum(axis=0)  # only difference


# merge self and others history vec
def merge_history(
    sum_history_test,
    test_key_set,
    training_sum_history_test,
    training_key_set,
    index, # neighborhood index
    alpha,
):
    merged_history = {}
    for test_key_id in range(len(test_key_set)):
        test_key = test_key_set[test_key_id]
        test_history = sum_history_test[test_key]
        sum_training_history = np.zeros(len(test_history))
        for indecis in index[test_key_id]:
            training_key = training_key_set[indecis]
            sum_training_history += training_sum_history_test[training_key]

        sum_training_history = sum_training_history / len(index[test_key_id])

        merge = test_history * alpha + sum_training_history * (1 - alpha)
        merged_history[test_key] = merge

    return merged_history


def merge_history_and_neighbors_future(
    future_data,
    sum_history_test,
    test_key_set,
    training_sum_history_test,
    training_key_set,
    index,
    alpha,
    beta,
):
    merged_history = {}
    for test_key_id in range(len(test_key_set)):
        test_key = test_key_set[test_key_id]
        test_history = sum_history_test[test_key]
        sum_training_history = np.zeros(len(test_history))
        sum_training_future = np.zeros(len(test_history))
        for indecis in index[test_key_id]:
            training_key = training_key_set[indecis]
            sum_training_history += training_sum_history_test[training_key]
            # future_vec = np.zeros((len(test_history)))
            for idx in future_data[training_key][1]:
                if idx >= 0:
                    sum_training_future[idx] += 1

        sum_training_history = sum_training_history / len(index[test_key_id])
        sum_training_future = sum_training_future / len(index[test_key_id])

        merge = (
            test_history * alpha + sum_training_history * (1 - alpha)
        ) * beta + sum_training_future * (1 - beta)
        merged_history[test_key] = merge

    return merged_history


def evaluate(
    data_chunk,
    training_key_set,
    test_key_set,
    input_size,
    group_size,
    within_decay_rate,
    group_decay_rate,
    num_nearest_neighbors,
    alpha,
    topk,
):
    activate_codes_num = -1
    # target_set built from training data
    logger.info('start temporal_decay_sum_history_training')
    temporal_decay_sum_history_training = temporal_decay_sum_history(
        data_chunk[training_chunk],
        training_key_set,
        input_size,
        group_size,
        within_decay_rate,
        group_decay_rate,
    )
    logger.info('end temporal_decay_sum_history_training')

    log_memory()
    gc.collect()
    log_memory()

    # query_set built from training data
    logger.info('start temporal_decay_sum_history_test')
    temporal_decay_sum_history_test = temporal_decay_sum_history(
        data_chunk[training_chunk],
        test_key_set,
        input_size,
        group_size,
        within_decay_rate,
        group_decay_rate,
    )
    logger.info('end temporal_decay_sum_history_test')

    log_memory()
    gc.collect()
    log_memory()

    logger.info('start knn')
    index, distance = KNN(
        temporal_decay_sum_history_test,
        temporal_decay_sum_history_training,
        num_nearest_neighbors,
    )
    logger.info('end knn')

    log_memory()
    gc.collect()
    log_memory()

    logger.info('start merge_history')
    sum_history = merge_history(
        temporal_decay_sum_history_test,
        test_key_set,
        temporal_decay_sum_history_training,
        training_key_set,
        index,
        alpha,
    )
    logger.info('end merge_history')

    log_memory()
    gc.collect()
    log_memory()

    if activate_codes_num < 0:
        # for i in range(1, 6):

        prec = []
        rec = []
        F = []
        prec1 = []
        rec1 = []
        F1 = []
        prec2 = []
        rec2 = []
        F2 = []
        prec3 = []
        rec3 = []
        F3 = []
        NDCG = []
        n_hit = 0

        num_ele = topk
        # logger.info('k = ' + str(activate_codes_num))
        # evaluate(data_chunk, input_size,test_KNN_history, test_key_set, next_k_step)
        count = 0
        for iter in range(len(test_key_set)):
            # training_pair = training_pairs[iter - 1]
            # input_variable = training_pair[0]
            # target_variable = training_pair[1]
            input_variable = data_chunk[training_chunk][test_key_set[iter]]
            target_variable = data_chunk[test_chunk][test_key_set[iter]]

            if len(target_variable) < 2 + next_k_step:
                continue
            count += 1
            output_vectors = predict_with_elements_in_input(
                sum_history, test_key_set[iter]
            )
            top = 400
            hit = 0
            for idx in range(len(output_vectors)):
                # for idx in [2]:

                output = np.zeros(input_size)
                target_topi = output_vectors[idx].argsort()[::-1][:top]
                c = 0
                for i in range(top):
                    if c >= num_ele:
                        break
                    output[target_topi[i]] = 1
                    c += 1

                vectorized_target = np.zeros(input_size)
                for ii in target_variable[1 + idx]:
                    vectorized_target[ii] = 1
                precision, recall, Fscore, correct = get_precision_recall_Fscore(
                    vectorized_target, output
                )
                prec.append(precision)
                rec.append(recall)
                F.append(Fscore)
                if idx == 0:
                    prec1.append(precision)
                    rec1.append(recall)
                    F1.append(Fscore)
                elif idx == 1:
                    prec2.append(precision)
                    rec2.append(recall)
                    F2.append(Fscore)
                elif idx == 2:
                    prec3.append(precision)
                    rec3.append(recall)
                    F3.append(Fscore)
                hit += get_HT(vectorized_target, target_topi, num_ele)
                ndcg = get_NDCG1(vectorized_target, target_topi, num_ele)
                NDCG.append(ndcg)
            if hit == next_k_step:
                n_hit += 1

        # logger.info('average precision of ' + ': ' + str(np.mean(prec)) + ' with std: ' + str(np.std(prec)))
        recall = np.mean(rec)
        ndcg = np.mean(NDCG)
        hr = n_hit / len(test_key_set)

    return recall, ndcg, hr

def evaluate_baseline(
    data_chunk,
    training_key_set,
    test_key_set,
    input_size,
    group_size,
    topk,
):

    logger.info('start sum_history')
    sum_history = get_sum_history(
        data_chunk[training_chunk],
        list(set(training_key_set + test_key_set)),
        input_size,
        group_size,
    )
    logger.info('end sum_history')

    prec = []
    rec = []
    F = []
    prec1 = []
    rec1 = []
    F1 = []
    prec2 = []
    rec2 = []
    F2 = []
    prec3 = []
    rec3 = []
    F3 = []
    NDCG = []
    n_hit = 0

    num_ele = topk
    # logger.info('k = ' + str(activate_codes_num))
    # evaluate(data_chunk, input_size,test_KNN_history, test_key_set, next_k_step)
    count = 0

    for iter in range(len(test_key_set)):
        input_variable = data_chunk[training_chunk][test_key_set[iter]]
        target_variable = data_chunk[test_chunk][test_key_set[iter]]

        if len(target_variable) < 2 + next_k_step:
            continue
        count += 1
        # predict_with_PIF
        output_vectors = predict_with_elements_in_input(sum_history, test_key_set[iter])

        top = 400
        hit = 0
        for idx in range(len(output_vectors)):
            output = np.zeros(input_size)
            target_topi = output_vectors[idx].argsort()[::-1][:top]
            c = 0
            for i in range(top):
                if c >= num_ele:
                    break
                output[target_topi[i]] = 1
                c += 1

            vectorized_target = np.zeros(input_size)
            for ii in target_variable[1 + idx]:
                vectorized_target[ii] = 1
            precision, recall, Fscore, correct = get_precision_recall_Fscore(
                vectorized_target, output
            )
            prec.append(precision)
            rec.append(recall)
            F.append(Fscore)
            if idx == 0:
                prec1.append(precision)
                rec1.append(recall)
                F1.append(Fscore)
            elif idx == 1:
                prec2.append(precision)
                rec2.append(recall)
                F2.append(Fscore)
            elif idx == 2:
                prec3.append(precision)
                rec3.append(recall)
                F3.append(Fscore)
            hit += get_HT(vectorized_target, target_topi, num_ele)
            ndcg = get_NDCG1(vectorized_target, target_topi, num_ele)
            NDCG.append(ndcg)
        if hit == next_k_step:
            n_hit += 1

    logger.info('average precision of ' + ': ' + str(np.mean(prec)) + ' with std: ' + str(np.std(prec)))
    recall = np.mean(rec)
    ndcg = np.mean(NDCG)
    hr = n_hit / len(test_key_set)

    return recall, ndcg, hr

def prepare_all_data(files):
    logger.info("start read_claim2vector_embedding_file_no_vector")
    (
        data_chunk,
        input_size,
        code_freq_at_first_claim,
    ) = read_claim2vector_embedding_file_no_vector(files)
    logger.info("end read_claim2vector_embedding_file_no_vector")

    logger.info("start dump orjson")
    with open("/mnt/data/ds/data/processed/pxmart__rec_data_chunk.json", "wb") as f:
        f.write(orjson.dumps(data_chunk, option=orjson.OPT_SERIALIZE_NUMPY))
    logger.info("end dump orjson")
    log_memory()

    return (
        data_chunk,
        input_size,
        code_freq_at_first_claim,
    )

def sampling(data_chunk, size):
    both_user_ids = list(
        set(
            list(data_chunk[test_chunk].keys()) + list(data_chunk[training_chunk].keys())
        )
    )
    random.shuffle(both_user_ids)
    # count = 0
    new_data_chunk = [{}, {}]
    for user_id in both_user_ids[:size]:
        # if data_chunk[training_chunk].get(user_id) and data_chunk[test_chunk].get(user_id):
        new_data_chunk[training_chunk][user_id] = data_chunk[training_chunk].get(user_id)
        new_data_chunk[test_chunk][user_id] = data_chunk[test_chunk].get(user_id)
        # count += 1
        # if count >= size:
        #     break
    del data_chunk
    data_chunk = new_data_chunk
    return data_chunk

def convert_internal_list_to_np(data_chunk):
    try:
        for fileid in [training_chunk, test_chunk]:
            for key in data_chunk[fileid]:
                for idx in range(len(data_chunk[fileid][key])):
                    data_chunk[fileid][key][idx] = np.array(data_chunk[fileid][key][idx])
    except:
        print(fileid, key)
        return

def load_all():
    with open(f"/mnt/data/ds/data/processed/pxmart__rec_data_chunk.json", "rb") as f:
        data_chunk = orjson.loads(f.read())
    return data_chunk

def load_from_sampling(size):
    with open(f"/mnt/data/ds/data/processed/pxmart__rec_data_chunk_{size}.json", "rb") as f:
        data_chunk = orjson.loads(f.read())
    return data_chunk

def save_from_sampling(data_chunk, size):
    data_chunk = sampling(data_chunk, size)
    with open(f"/mnt/data/ds/data/processed/pxmart__rec_data_chunk_{size}.json", "wb") as f:
        f.write(orjson.dumps(data_chunk, option=orjson.OPT_SERIALIZE_NUMPY))

def make_sampling(size):
    data_chunk = load_all()
    save_from_sampling(data_chunk, size)


def main(argv):

    # files = [argv[1], argv[2]]
    # files = ['/mnt/data/ds/data/raw/pxmart__rec_dateset_sorted_history.csv', '/mnt/data/ds/data/raw/pxmart__rec_dateset_sorted_future.csv']
    # (
    #     data_chunk,
    #     input_size,
    #     code_freq_at_first_claim,
    # ) = prepare_all_data(files)
    # log_memory()


    # calculated by pxmart__rec_data_chunk.json
    num_dim = 27881
    input_size = num_dim + 2

    sampling_size = 20*10000

    data_chunk = load_from_sampling(sampling_size)


    # clean up diff user_id (test exist but train not exist)
    test_user_ids = list(data_chunk[test_chunk].keys())

    # remove diff (test - train)
    for user_id in test_user_ids:
        if not data_chunk[training_chunk].get(user_id):
            del data_chunk[test_chunk][user_id]

    for user_id in list(data_chunk[training_chunk].keys()):
        if not data_chunk[training_chunk].get(user_id):
            del data_chunk[training_chunk][user_id]

    # cleanup
    test_user_ids = list(data_chunk[test_chunk].keys())
    for user_id in test_user_ids:
        if not data_chunk[training_chunk].get(user_id):
            del data_chunk[training_chunk][user_id]
        if not data_chunk[test_chunk].get(user_id):
            del data_chunk[test_chunk][user_id]


    logger.info("start convert numpy array")
    convert_internal_list_to_np(data_chunk)
    logger.info("end convert numpy array")
    log_memory()

    gc.collect()
    logger.info(f'user_id {len(data_chunk[test_chunk].keys())} {len(data_chunk[training_chunk].keys())}')
    log_memory()

    logger.info("start partition_the_data_validate")
    training_key_set, validation_key_set, test_key_set = partition_the_data_validate(
        data_chunk, list(data_chunk[test_chunk]), 1
    )
    logger.info("end partition_the_data_validate")
    log_memory()

    # num_nearest_neighbors = int(argv[3])
    # within_decay_rate = float(argv[4])
    # group_decay_rate = float(argv[5])
    # alpha = float(argv[6])
    # group_size = int(argv[7])
    # topk = int(argv[8])

    num_nearest_neighbors = 300
    within_decay_rate = 0.9
    group_decay_rate = 0.7
    alpha = 0.7
    group_size = 7
    topk = 10

    logger.info("start evaluate")

    logger.info(f"Num. of top: {topk}")
    recall, ndcg, hr = evaluate(
        data_chunk,
        training_key_set,
        test_key_set,
        input_size,
        group_size,
        within_decay_rate,
        group_decay_rate,
        num_nearest_neighbors,
        alpha,
        topk,
    )
    logger.info("===TIFU-KNN===")
    logger.info(f"recall: {str(recall)}")
    logger.info(f"NDCG: {str(ndcg)}")
    logger.info(f"hit ratio: {str(hr)}")
    logger.info("end evaluate")


    logger.info("===PersonTopFreq===")
    logger.info("start evaluate")
    logger.info(f"Num. of top: {topk}")
    # recall, ndcg, hr = evaluate_baseline(data_chunk, training_key_set, test_key_set, input_size,
    #                             group_size, topk)

    logger.info(f"recall: {str(recall)}")
    logger.info(f"NDCG: {str(ndcg)}")
    logger.info(f"hit ratio: {str(hr)}")

    logger.info("end evaluate")



if __name__ == "__main__":
    main(sys.argv)
