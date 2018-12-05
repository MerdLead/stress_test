import multiprocessing
import random
import re
from multiprocessing.pool import Pool

from stress_point.common import CommonAction
from conf import config


class HomeWorkAction:
    """
    作业测试点：
    1、复制习题，创建作业，表: user_quoted_testbank、homework
    2、将作业布置给学生 表: vanclass_student_homework、teacher_exercise_overview 、testbank_quote_record
    3、手动做一题，复制做题记录到作业的其他题目 表:homework_student_overview、homework_student_record、homework_student_wrong
    """

    def __init__(self):
        self.common = CommonAction()

    def add_quote_to_homework(self, bank_array, i):
        """添加复制来的习题到作业"""
        index = '' if i == 0 else str(i)
        print(config.HOMEWORK_NAME + index)
        self.common.add_homework(config.HOMEWORK_NAME + index, config.TEACHER_ID, config.CLASS_ID, bank_array)

    def get_done_homework_id_list(self):
        """获取测试学生已做的作业id"""
        done_object_ids = self.common.find_done_homework_id(config.STU_ID)
        done_ids_list = list(map(lambda x: x[0], done_object_ids))
        return done_ids_list

    def get_all_homework_id(self):
        object_id = self.common.find_homework_id(config.HOMEWORK_NAME)
        object_list = list(map(lambda x: x[0], object_id))
        return object_list

    def get_new_homework_id(self):
        """获取作业id """
        all_object_id = self.get_all_homework_id()  # 所有的类似名称的作业
        done_object_id = self.get_done_homework_id_list()  # 已经做过的作业名称
        done_id_array = [] if len(done_object_id) == 1 else done_object_id
        if all_object_id == done_id_array:
            new_object_id = done_id_array
        else:
            new_object_id = list(set(all_object_id).difference(set(done_id_array)))  # 取两个数组的差集获得未做作业集
        return new_object_id

    # 作业清除
    def delete_homework_detail_info(self, homework_id):
        """删除学生的作业做题记录"""
        self.common.delete_homework_wrong(homework_id)
        self.common.delete_homework_overview(homework_id)
        self.common.delete_homework_record(homework_id)
        self.common.delete_testbank_quote_record(homework_id)
        self.common.delete_exercise_overview(homework_id)
        self.common.delete_class_stu_homework(homework_id)
        self.common.delete_homework(homework_id)


    def copy_quote_to_homework(self):
        print("创建作业，加入习题")
        bank_array = ','.join(self.common.get_bank_array())  # 获取习题id
        homework_name_list = self.common.find_homework_name(config.HOMEWORK_NAME)
        max_index = self.common.get_object_max_index(homework_name_list)

        for i in range(max_index, config.HOMEWORK_NUM + max_index):
            self.add_quote_to_homework(bank_array, i)  # 创建作业 将原题与复制习题加入作业

    def get_one_student_done_info(self):
        """获取测试学生的已做习题记录"""
        done_homework_list = self.get_done_homework_id_list()
        done_record = self.common.find_done_homework_record(config.STU_ID, done_homework_list[0])[0]
        done_overview = self.common.find_done_homework_overview(config.STU_ID, done_homework_list[0])[0]
        done_wrong = self.common.find_done_wrong_by_record_id(done_record[0], config.STU_ID)
        return done_record, done_overview, done_wrong, len(done_homework_list)

    def delete_student_homework_info(self):
        print("清除上次作业数据")
        homework_list = self.get_all_homework_id()
        for homework_id in homework_list:
            print(homework_id)
            self.delete_homework_detail_info(homework_id)
        self.common.delete_quotebank_info()


    def add_homework_to_students(self, student_list, bank_array, homework_list):
        print("给班级学生布置作业")
        for homework_id in homework_list:
            for stu_id in student_list:  # 给班级学会添加作业
                print(stu_id, homework_id)
                self.common.add_vanclass_homework(config.CLASS_ID, stu_id, homework_id, config.TEACHER_ID)

            for j in range(len(bank_array)):
                self.common.add_quoted_bank_record(bank_array[j], 'homework', homework_id, config.HOMEWORK_NAME)  # 给每一题添record
            self.common.add_teacher_exercise_overview('homework_normal', homework_id, config.HOMEWORK_NAME)  # 在overview表中添加数据


    def complete_done_info(self, homework_id, student_id, bank_array, done_record, done_overview, done_wrong):
        for i in range(len(bank_array)):
            self.common.add_homework_record(done_record, homework_id, student_id, bank_array[i])      # 添加作业记录
            self.common.add_homework_overview(done_overview, homework_id, student_id, bank_array[i])  # 添加overview

            record_id = self.common.find_done_record_id_by_bank_id(homework_id, bank_array[i])[0][0]
            print(bank_array[i])
            for j in range(len(done_wrong)):    # 依次添加错题记录
                self.common.add_homework_wrong(done_wrong[j], homework_id, student_id, record_id, bank_array[i])


    def copy_done_info_to_others(self, student_list, bank_array, homework_list):
        print("复制记录至其他习题和其他人")
        done_info = self.get_one_student_done_info()
        done_record = done_info[0]   # 做题记录
        done_overview = done_info[1]  # 正确题目
        done_wrong = done_info[2]     # 错误题目
        done_homework_num = done_info[3]

        done_bank_id = str(done_record[4])  # 已做大题的习题id
        done_homework_id = done_record[2]   # 已做作业的id

        for homework_id in homework_list:
            for student_id in student_list:
                print(homework_id, student_id)
                if homework_id == done_homework_id and student_id == config.STU_ID:
                    if done_homework_num == 1:
                        bank_array.remove(done_bank_id)
                        self.complete_done_info(homework_id, student_id, bank_array, done_record, done_overview,done_wrong)
                        bank_array.insert(0, done_bank_id)
                else:
                    self.complete_done_info(homework_id, student_id, bank_array, done_record, done_overview,done_wrong)


    def update_homework_completion(self, homework_list):
        """第四步， 更改作业完成度"""
        print("更改作业完成度")
        for ids in homework_list:
            print(ids)
            self.common.update_complete_rate(ids)



    def run(self):
        # 清除上次数据
        self.delete_student_homework_info()

        # 给班级添加学生
        # self.add_stu_to_class()
        #
        # # 更改配置，重新创建作业
        self.common.add_quote_bank()  # 添加复制习题
        self.copy_quote_to_homework()  # 创建作业

        # # 获取大题、作业、学生列表
        bank_array = self.common.get_bank_array()
        homework_list = self.get_new_homework_id()
        student_list = self.common.get_class_student()
        # student_list = ['52385']
        #
        # # 给学生添加作业
        self.add_homework_to_students(student_list, bank_array, homework_list)

        # 手动做某一试卷的一题，然后进行复制
        # self.copy_done_info_to_others(student_list, bank_array, homework_list)

        # 更改作业完成度
        # self.update_homework_completion(homework_list)


if __name__ == '__main__':
    home = HomeWorkAction()
    # home.get_one_student_done_info()
    p = Pool(4)
    p.apply_async(home.run())
    p.close()
    home.common.close_db()
