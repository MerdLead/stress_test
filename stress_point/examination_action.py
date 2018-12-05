from multiprocessing.pool import Pool

import pymysql

from stress_point.common import CommonAction
from conf import config



class ExamAction:

    def __init__(self):
        self.common = CommonAction()

    def add_copy_bank(self, quote):
        for i in range(0, config.BANK_NUM):
            self.common.add_found_quot_copy(quote[0], i)

    def add_quote_bank_entity(self, bank_array, first_bank_id):
        """添加复制习题 信息"""
        entity = self.common.find_quote_entity(first_bank_id)
        for i in range(1, len(bank_array)):
            for j in range(len(entity)):
                self.common.add_quote_bank_entity(bank_array[i], entity[j])

    def get_all_quotation_id(self):
        """获取以布置试卷的id"""
        quote_id = self.common.find_test_quotation_ids(config.EXAM_NAME)
        quote_list = list(map(lambda x: x[0], quote_id))
        return quote_list

    def get_done_quotation_id_list(self):
        """获取测试学生已做的作业id"""
        done_object_ids = self.common.find_done_quote_id(config.STU_ID)
        done_ids_list = list(map(lambda x: x[0], done_object_ids))
        return done_ids_list

    def get_new_quote_id(self):
        """获取作业id """
        all_object_id = self.get_all_quotation_id()  # 所有的类似名称的作业
        done_object_id = self.get_done_quotation_id_list()  # 已经做过的作业名称
        done_id_array = [] if len(done_object_id) == 1 else done_object_id
        if all_object_id == done_id_array:
            new_object_id = done_id_array
        else:
            new_object_id = list(set(all_object_id).difference(set(done_id_array)))  # 取两个数组的差集获得未做作业集
        return new_object_id

    def add_quotation(self, exam_info, name, quote_list):
        """添加quotation表数据"""
        if len(quote_list) == 0:
            origin_quotation_id = pymysql.NULL
        else:
            origin_quotation_id = quote_list[0]
        self.common.add_test_quotation(exam_info, name, origin_quotation_id, config.CLASS_ID)

    def add_quote_record_and_map(self, bank_array, object_type, object_id, name, score):
        """为所有的大题添加map与record数据"""
        for i in range(len(bank_array)):
            self.common.add_quoted_map(object_type, object_id, bank_array[i], score, i + 1)
            self.common.add_quoted_bank_record(bank_array[i], object_type, object_id, name)

    def get_done_wrong_answer(self, item_id):
        """获取已做习题的错误答案"""
        result = self.common.find_done_wrong_by_section_id(item_id)
        return result[0]

    @staticmethod
    def get_section_ids_list(section_ids, wrong_count):
        """转换错误id，将str类型转为list类型"""
        wrong_id_array = section_ids[0].split(',')[:wrong_count]
        right_count = section_ids[2]
        if right_count == 0:
            right_id_array = section_ids[0].split(',')[wrong_count:]
        else:
            right_id_array = section_ids[1].split(',')
        wrong_ids = ','.join(wrong_id_array)
        right_ids = ','.join(right_id_array)
        return wrong_ids, right_ids, wrong_id_array, right_id_array

    def delete_no_do_right(self, right_id):
        """删除错题中正确题目的id 对应的数据"""
        self.common.delete_no_do_right_answer(right_id)

    def change_exam_score(self, quote_id):
        """更改 试卷最终分数"""
        score_result = self.common.find_testbank_score(quote_id)
        score_list = list(map(lambda x: x[0], score_result))
        total_score = score_list[0] * len(score_list)
        self.common.update_exam_student_score(total_score, quote_id, config.STU_ID)
        return total_score

    def get_student_record(self, quote_id, student_id):
        """获取学生record表数据"""
        student_record = self.common.find_one_stu_exam_record(quote_id, student_id)
        record_list = list(map(lambda x: x[0], student_record))
        return student_record, record_list

    def get_one_stu_wrong(self, quote_id):
        """获取某一学生错题记录"""
        student_wrong = self.common.find_one_stu_exam_wrong(quote_id)
        return student_wrong


    """
    学生做试卷步骤主要分为以下几步
    """
    def delete_test_info(self):
        """清除保留在题库的试卷信息"""
        self.common.delete_test(config.EXAM_NAME)

    def delete_quotation_info(self):
        """清除布置出去的试卷信息"""
        exam_list = self.get_all_quotation_id()
        for exam_id in exam_list:
            print("删除试卷id:", exam_id)
            self.common.delete_exam_wrong(exam_id)
            self.common.delete_exam_record(exam_id)
            self.common.delete_exercise_overview(exam_id)
            self.common.delete_student_exam(exam_id)
            self.common.delete_test_quotation(exam_id)
            self.common.delete_testbank_quote_record(exam_id)

    def delete_quote_bank_info(self):
        bank_array = self.common.get_bank_array()
        print("复制习题id:", bank_array)
        for i in range(1, len(bank_array)):
            self.common.delete_quote_entity(bank_array[i])
            self.common.delete_quote_bank(bank_array[i])

        for i in range(0, len(bank_array)):
            self.common.delete_quote_map(bank_array[i])

    def delete_all_exam_info(self):
        print("删除所有考试信息")
        self.delete_quotation_info()
        self.delete_test_info()
        self.delete_quote_bank_info()


    def add_test(self, bank_array, score):
        bank_info = ','.join(bank_array)  # 将列表组成字符串
        bank_item_count = self.common.find_testbank_item_count(config.QUOTE_NAME)[0][0]  # 获取其中一大题题目个数
        exam_item_count = config.BANK_NUM * bank_item_count  # 试卷中总的个数
        self.common.add_test(config.EXAM_NAME, bank_info, exam_item_count, config.TEACHER_ID)  # 创建test表中的试卷数据
        test_info = self.common.find_exam_info(config.EXAM_NAME)[0]  # 获取刚刚创建的test数据
        self.add_quote_record_and_map(bank_array, 'test', test_info[0], config.EXAM_NAME, score)  # 添加map和record表
        return test_info

    def add_copy_quote_to_exam(self, quote):
        print('复制习题，创建试卷')

        bank_array = self.common.get_bank_array()    # 获取复制习题列表
        print('大题id：', bank_array)
        # self.add_quote_bank_entity(bank_array, quote[0][0])  # 添加entity表数据

        score = int(100 / (config.BANK_NUM + 1))  # 每一题的总分数
        # self.add_test(bank_array, score)

        test_info = self.common.find_exam_info(config.EXAM_NAME)[0]  # 获取刚刚创建的test数据
        exam_name_list = self.common.find_exam_name(config.EXAM_NAME)
        max_index = self.common.get_object_max_index(exam_name_list)

        for i in range(max_index, config.EXAM_NUM + max_index):
            name = config.EXAM_NAME if i == 0 else config.EXAM_NAME + str(i)  # 根据情况获取quote名称
            quote_list = self.get_all_quotation_id()
            self.add_quotation(test_info, name, quote_list)    # 利用刚创建的test创建quotation
            quote_id = self.common.find_test_quotation_ids(name)[0][0]
            self.add_quote_record_and_map(bank_array, 'exam', quote_id, name, score)  # 添加map和record表
            self.common.add_teacher_exercise_overview('exam', quote_id, name)   # 添加teacher_exercise_overview表
            print(name)


    def add_student_exam(self, student_list, quote_list):
        print('布置试卷给学生')
        for student_id in student_list:
            for quote_id in quote_list:
                print(student_id, quote_id)
                self.common.add_exam_student(config.CLASS_ID, student_id, quote_id, config.TEACHER_ID)


    # 更改未做题的record表数据
    def copy_done_tip_to_not_do(self, done_info, exam_id, bank_array):
        """第三步， 手动做试卷某一小题后提交，然后对已做习题的做题记录进行复制"""
        print('将一道大题复制到试卷的其他题目上')
        done_wrong_array = done_info[7].split(',')   # 获取已做错题
        total_count = (config.BANK_NUM + 1) * done_info[4]  # 获取总题数
        score = int(100 / total_count * done_info[8])  # 单词分数
        wrong_count = done_info[6]   # 错误题数

        for i in range(1, len(bank_array)):
            section_ids = self.common.find_no_done_item_ids(exam_id, bank_array[i])[0]   # 未做习题的section_id
            section_list = self.get_section_ids_list(section_ids, wrong_count)   # 将id进行分解
            self.common.update_no_exercise_record(score, done_info[6], section_list[0], done_info[8], section_list[1],
                                                  exam_id, bank_array[i])   # 更改未做习题的record数据

            for j in range(len(done_wrong_array)):
                wrong_tip_answer = self.get_done_wrong_answer(done_wrong_array[j])   # 错题额答案
                self.common.update_no_do_wrong(wrong_tip_answer[0], wrong_tip_answer[1], section_list[2][j])

            right_ids = section_list[3]
            for k in range(len(right_ids)):
                self.delete_no_do_right(right_ids[k])

    def copy_one_stu_to_others(self, student_record, student_wrong, quote_id, student_id):
        """将某一学生做题记录复制给其他学生"""
        wrong_count = student_record[0][6]
        for j in range(len(student_record)):
            self.common.add_exam_student_record(student_record[j], quote_id, student_id)

        others_record_id_list = self.get_student_record(quote_id, student_id)[1]   # 获取新创建的record id
        record_array = others_record_id_list * wrong_count
        record_array.sort()
        for m in range(len(student_wrong)):
            self.common.add_exam_student_wrong(student_wrong[m], quote_id, student_id, record_array[m])

    def complete_exam_for_others(self, bank_array, student_list, test_quote_list):
        done_info = self.common.find_done_quote_record(config.STU_ID)
        done_exam_id = done_info[0][1]
        if len(done_info) == 1:
            self.copy_done_tip_to_not_do(done_info[0], done_exam_id, bank_array)
        total_score = self.change_exam_score(done_exam_id)

        student_record = self.get_student_record(done_exam_id, config.STU_ID)[0]
        student_wrong = self.get_one_stu_wrong(done_exam_id)

        for quote_id in test_quote_list:
            for student_id in student_list:
                if done_exam_id == quote_id and student_id == config.STU_ID:
                    continue
                self.copy_one_stu_to_others(student_record, student_wrong, quote_id, student_id)
                self.common.update_exam_student_score(total_score, quote_id, student_id)

    def run(self):
        # 删除上一次数据
        # self.delete_all_exam_info()

        # 复制习题 创建试卷
        quote = self.common.find_ready_quote_by_name(config.QUOTE_NAME)
        # self.add_copy_bank(quote)
        # self.add_copy_quote_to_exam(quote)

        # 添加班级学生
        # self.common.add_stu_to_class()
        bank_array = self.common.get_bank_array()
        # student_list = self.common.get_class_student()
        student_list = ['52385']
        test_quote_list = self.get_new_quote_id()

        # 为班级学生添加试卷
        # self.add_student_exam(student_list, test_quote_list)
        self.complete_exam_for_others(bank_array, student_list, test_quote_list)


if __name__ == '__main__':
    data = ExamAction()
    p = Pool(4)
    p.apply_async(data.run())
    p.close()
    data.common.close_db()
    # data.exam.close_db()
