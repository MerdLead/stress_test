import random
from conf import config
from util.sql_collection import SQLCollection



class CommonAction(SQLCollection):

    def add_quote_bank(self):
        quote = self.find_ready_quote_by_name(config.QUOTE_NAME)
        for i in range(0, config.BANK_NUM):
            self.add_found_quot_copy(quote[0], i)
        return quote[0][0]

    def get_bank_array(self):
        """获取题目的id"""
        banks = self.find_copy_testbank(config.QUOTE_NAME)
        bank_array = list(map(lambda x: str(x[0]), banks))
        return bank_array

    @staticmethod
    def get_object_max_index(object_name_list):
        """获取当前数据库中作业身后数字最大的数字"""
        home_index = []
        for i in range(len(object_name_list)):
            index = object_name_list[i][0][-1]
            if index.isdigit():
                home_index.append(int(index))
            else:
                continue
        max_index = 0 if len(home_index) == 0 else home_index[-1]
        index = 0 if max_index == 0 else max_index + 1
        return index


    def add_stu_to_class(self):
        """为班级添加学生"""
        stu_info = self.find_all_student()
        stu_array = random.sample(stu_info, config.STU_NUM)
        for stu in stu_array:
            self.add_student_class(config.CLASS_ID, stu[0], stu[1])

    def get_class_student(self):
        """获取班级所有学生id"""
        all_student = self.find_class_student(config.CLASS_ID)
        student_list = list(map(lambda x: str(x[0]), all_student))
        return student_list

    def add_teacher_exercise_overview(self, object_type, object_id, object_name):
        """"添加 exercise_overview表数据"""
        self.add_exercise_overview(config.TEACHER_ID, config.CLASS_ID, object_id, object_type, object_name)

    def add_quoted_bank_record(self, bank_id, object_type, object_id, object_name):
        """添加习题记录"""
        self.add_quote_record(bank_id, config.TEACHER_ID, object_type, object_id, object_name)

    def delete_quotebank_info(self):
        """删除复制的习题信息"""
        bank_array = self.get_bank_array()
        for i in range(1, len(bank_array)):
            self.delete_quote_bank(bank_array[i])

