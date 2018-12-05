import random
from multiprocessing.pool import Pool

from conf import config
from stress_point.common import CommonAction
from stress_point.homework_action import HomeWorkAction


class WordBookAction:

    def __init__(self):
        self.common = CommonAction()

    def get_label_id(self):
        """获取所有标签列表"""
        have_label_ids = self.common.find_student_have_label_ids(config.STU_ID)
        have_label_list = list(map(lambda x: int(x[0]), have_label_ids))
        have_label_list.sort()
        result = self.common.find_label_id()
        ids = random.sample(result, config.LABEL_NUM)
        id_list = list(map(lambda x: x[0], ids))
        id_list.sort()

        same_ele_list = list(set(id_list).intersection(set(have_label_list)))  # 新得到的词书数组与已拥有的词书数组取其交集
        final_label_list = list(set(id_list).difference(set(same_ele_list)))  # 取新得到的词书数组与交集数组的差集
        final_label_list.sort()
        return final_label_list,have_label_list

    def get_sys_label_id(self):
        """获取对应名称的系统标签"""
        label_id = self.common.find_sys_label_by_name(config.GRADE)
        return label_id[0][0]

    def get_wordbank_label_id(self, sys_label):
        """获取系统标签对应的wordbank_id"""
        label_id = self.common.find_wordbank_label_by_sys_label(sys_label)
        return label_id[0]

    def get_sys_wordbank_id(self):
        """获取所有系统标签对应的wordbank_id"""
        result = self.common.find_sys_wordbank_id()
        sys_wordbank_list = list(map(lambda x: x[0], result))
        return sys_wordbank_list

    def get_sys_homework_id(self, wordbank_label_id):
        """根据系统的wordbank_id获取homework_id"""
        result = self.common.find_sys_homework_id_by_wordbank(wordbank_label_id)
        return result[0][0]

    def get_homework_id_by_name(self, name):
        """获取标签的名称"""
        result = self.common.find_word_homework_id(config.TEACHER_ID, name)
        return result

    def get_label_word_list(self, label_id):
        """获取标签下所有的单词列表"""
        content = self.common.find_word_by_label(label_id)
        word_list = content[0][0].split(',')
        return word_list

    def add_word_student_fluency(self, student_id, word_homework_id, is_system, label_id, level):
        """添加系统标签下的单词到student的word_fluency表中"""
        word_list = self.get_label_word_list(label_id)
        for i in range(len(word_list)):
            self.common.add_word_fluency(student_id, word_list[i], word_homework_id, is_system, label_id, level)


    def delete_word_data(self, student_list):
        """删除学生单词数据"""
        for student_id in student_list:
            self.common.delete_student_word_fluency(student_id)
            self.common.delete_student_word_homework(student_id)
            self.common.delete_word_homework_record(student_id)
            student_label_id = self.common.find_data_studnet_label_id(student_id)
            if len(student_label_id) != 0:
                self.common.delete_student_word_data(student_id, 'student_label_id')
            fluency_id = self.common.find_fluency_id_by_student_id(student_id)
            self.common.delete_student_fluency_record(fluency_id)

        self.common.delete_before_created_homework(config.CLASS_ID)
        
    def get_student_fluency_ids(self, student_id, label_id):
        """获取学生word_fluency下所有对应标签的id"""
        result = self.common.find_student_fluency_id(student_id, label_id)
        fluency_id_list = list(map(lambda x: x[0], result))
        wordbank_id_list = list(map(lambda x: x[1], result))
        return fluency_id_list, wordbank_id_list

    def add_student_fluency_record(self, fluency_list):
        """添加fluency_record数据"""
        for sfi in fluency_list:
            self.common.add_word_student_fluency_record(sfi)

    def add_word_homework_student_record(self, student_id, fluency_list, wordbank_list):
        """添加word_homework_record数据（学生查看词书进度用得到）"""
        fluency_ids = ','.join(fluency_list)
        wordbank_ids = ','.join(wordbank_list)
        self.common.add_word_homework_student_record(student_id, wordbank_ids, fluency_ids, len(fluency_list))

    def get_label_name_parent_id(self, label_id):
        """获取标签的父级id"""
        result = self.common.fins_label_name_parent(label_id)
        return result[0]


    def get_homework_name(self, label_id):
        """由当前标签依次向上追溯，直到没有父级为止，并取他们的名称依次存入数组后，
        再进行倒叙排列，得到新的作业名称"""
        name_parts = []
        value = label_id
        while True:
            parent = self.get_label_name_parent_id(value)
            if parent[1] != 0:
                name_parts.append(parent[0])
                value = parent[1]
            else:
                break
        name_parts.reverse()
        name = '-'.join(name_parts)
        return name

    def add_word_homework(self, label_lsit, sys_wordbank_id):
        """获取标签和对应的作业"""

        label_homework_info = {}
        for i in range(len(label_lsit)):
            if label_lsit[i] in sys_wordbank_id:   # 若标签在系统标签中，可由label_student_wordbank_map中获取homework_id
                sys_homework_id = self.get_sys_homework_id(label_lsit[i])
                label_homework_info[label_lsit[i]] = sys_homework_id
            else:
                homework_name = self.get_homework_name(label_lsit[i])  # 新的作业名称
                homework_id = self.get_homework_id_by_name(homework_name)  # 作业id
                if len(homework_id) == 0:
                    self.common.add_word_homework(homework_name, config.TEACHER_ID, config.CLASS_ID)  # 添加作业
                    homework_id = self.get_homework_id_by_name(homework_name)  # 重新获取homework_id

                label_homework_info[label_lsit[i]] = homework_id[0][0]

        print("词书与作业的对应关系:", label_homework_info)
        return label_homework_info

    def add_student_word_data(self, student_list, label_list, sys_wordbank_id, label_homework_info, sys_label):
        """将单词布置给学生"""
        for student_id in student_list: 
            self.common.add_user_student_data(student_id, 'student_label_id', sys_label)  # 添加key为student_label_id数据
            for label_id in label_list:
                print('学生id', student_id, '词书id', label_id, ' 作业id', label_homework_info[label_id])
                result = self.get_student_fluency_ids(student_id, label_id)  # 学生下的fluency_record
                fluency_id_list = result[0]
                wordbank_id_list = result[1]
                is_system = 1 if label_id in sys_wordbank_id else 0  # is_system 判断

                self.common.add_word_homework_student(config.CLASS_ID, student_id, label_homework_info[label_id], label_id,
                                                      is_system)

                self.add_word_student_fluency(student_id, label_homework_info[label_id], is_system, label_id, 1)

                self.add_student_fluency_record(fluency_id_list)  # 添加fluency_record数据

                self.add_word_homework_student_record(student_id, fluency_id_list, wordbank_id_list)

    def run(self):
        student_list = ['52385']
        # student_list = self.common.get_class_student()   # 学生列表
        labels = self.get_label_id()
        new_label_list = labels[0]   # 标签列表
        old_label_list = labels[1]
        sys_label = self.get_sys_label_id()  # 系统标签列表
        sys_homework_id = self.get_wordbank_label_id(sys_label)[0]  # 系统标签对应的作业id和wordbank_id
        sys_wordbank_id = self.get_sys_wordbank_id()  # 系统worbank_id
        if sys_homework_id not in new_label_list:  # 若获取的label列表中没有系统对应的wordbank_id,添加到label列表中
            new_label_list.append(sys_homework_id)

        if sys_homework_id in old_label_list:
            new_label_list.remove(sys_homework_id)

        print('词书id:', new_label_list)


        # 删除学生单词数据
        # self.delete_word_data(student_list)

        # 添加或获取词书对应的作业
        label_homework_info = self.add_word_homework(new_label_list, sys_wordbank_id)  # 获取到的label与homework，以dict形式

        # 给学生添加单词
        self.add_student_word_data(student_list, new_label_list, sys_wordbank_id, label_homework_info, sys_label)


if __name__ == '__main__':
    wordbook = WordBookAction()
    p = Pool()
    p.apply_async(wordbook.run())
    p.close()
    wordbook.common.close_db()


