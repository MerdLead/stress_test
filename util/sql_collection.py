import datetime

import pymysql

from util.mysql_action import MysqlAction


class SQLCollection(MysqlAction):
    """作业部分"""

# ===================================== 作业部分 ============================================================
# 为班级添加新的学生
    def find_ready_quote_by_name(self, name):
        sql = 'SELECT * FROM `user_quoted_testbank` WHERE `name` like \'%{}%\''.format(name)
        return self.execute_sql_return_result(sql)

    def find_all_student(self):
        """查询所有学生"""
        sql = "SELECT `id`,`nickname` FROM user_account WHERE user_type_id ='5' AND is_available ='1'"
        return self.execute_sql_return_result(sql)

    def add_student_class(self, class_id, stu_id, stu_name):
        """将学生id 与姓名添加到班级表中"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = 'INSERT INTO `vanclass_student`(vanclass_id,student_id,mark_name,joined_time,is_active,sort,created_at,' \
              'updated_at) VALUES("{0}","{1}","{2}","{3}",1,0,"{4}","{5}")'\
              .format(class_id, stu_id, stu_name, now, now, now)
        self.execute_sql_only(sql)

# 在作业中添加复制来的题目
    def find_one_quot(self, quote_id):
        """获取目标题目的所有信息 """
        sql = 'SELECT * FROM user_quoted_testbank WHERE id= "{}"'.format(quote_id,)
        return self.execute_sql_return_result(sql)

    def add_found_quot_copy(self, q, i):
        """复制已知题目，只更改名称，其他保留"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = 'INSERT INTO `user_quoted_testbank`' \
              '(`name`, same_name_index, keyword, description, game_id, game_type_id, `mode`,game_mode_id, ' \
              'origin_type, origin_id,  item_count, item_ids, favorite_num, account_id,created_at, updated_at)'\
            'VALUES ("{0}","0","","","{2}","{3}","{4}","{5}","{6}","{7}","{8}","{9}","{10}","{11}","{12}","{13}")'.\
            format(q[1]+str(i+1), q[4], q[5], q[6], q[7], q[8], q[9], q[10], q[14], q[15], q[16], q[17],
                   now, now)
        self.execute_sql_only(sql)

    def find_copy_testbank(self, quote_name):
        """查询所有已复制过的题目"""
        sql = "SELECT `id` FROM `user_quoted_testbank` WHERE `name` like '%{}%'" .format(quote_name)
        return self.execute_sql_return_result(sql)

    def add_homework(self, name, teacher_id, class_id, testbank_array):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "insert into `homework` (`name`,`teacher_id`,`mode`, `homework_type_id`,  `testbank_array`, " \
              "`public_vanclass_ids`,`status`, `updated_at`, `created_at`) values ('{0}', '{1}', 'normal', '1'," \
              "'{2}', '{3}', 1, '{4}','{5}')".format(name, teacher_id, testbank_array, class_id, now, now)
        self.execute_sql_only(sql)

    def update_homework(self, testbank_array):
        """更改 作业题目个数"""
        sql = 'UPDATE `homework` SET testbank_array = "2798573,2798575" WHERE id = "%s"' %testbank_array
        self.execute_sql_only(sql)

# 给班级学生添加作业
    def find_class_student(self, class_id):
        """查询班级下所有学生"""
        sql = 'SELECT student_id FROM `vanclass_student` WHERE vanclass_id = "%s"' % class_id
        return self.execute_sql_return_result(sql)

    def find_homework_id(self, homework_name):
        sql = 'SELECT `id` FROM homework WHERE `name` like "%{}%"' .format(homework_name)
        return self.execute_sql_return_result(sql)

    def find_homework_name(self, homework_name):
        sql = 'SELECT `name` FROM homework WHERE `name` like "%{}%"'.format(homework_name)
        return self.execute_sql_return_result(sql)

    def add_vanclass_homework(self, class_id, student_id, homework_id, teacher_id):
        """为班级学生添加作业"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "insert into `vanclass_student_homework` (`vanclass_id`, `student_id`, `homework_id`, `teacher_id`, " \
              "`is_finish`,`is_finish_for_teacher`,`created_at`, `updated_at`)" \
              "values('{0}', '{1}', '{2}', '{3}', 0, 0 ,'{4}','{5}')".\
             format(class_id, student_id, homework_id, teacher_id, now, now)
        self.execute_sql_only(sql)

    def add_exercise_overview(self, teacher_id, class_id, exercise_id, exercise_type, exercise_name):
        """添加练习记录 """
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "insert into `teacher_exercise_overview` (`teacher_id`,`vanclass_id`,`exercise_id`, `exercise_type`, " \
              "`exercise_name`,`created_at`, `updated_at`) values('{0}', '{1}', '{2}','{3}'," \
              "'{4}','{5}','{6}')".format(teacher_id, class_id, exercise_id, exercise_type, exercise_name, now, now)
        self.execute_sql_only(sql)

    def add_quote_record(self, testbank_id, teacher_id, object_type, object_id, object_name):
        """添加题目记录"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "insert into `testbank_quote_record` (`testbank_type`,`testbank_id`, `account_id`, `object_type`," \
              "`object_id`, `object_name`,`created_at`,  `updated_at`) values ('quotedTestbank','{0}','{1}','{2}'," \
              "'{3}','{4}','{5}','{6}')".format(testbank_id, teacher_id, object_type, object_id, object_name, now, now)
        self.execute_sql_only(sql)

# 复制某一学生做题记录到其他学生上
    def find_homework_done_student(self, class_id, homework_id):
        sql = 'SELECT `student_id` FROM `vanclass_student_homework` WHERE vanclass_id ="{0}" ' \
              'and homework_id = "{1}" AND completion_rate != 0'.format(class_id, homework_id)
        return self.execute_sql_return_result(sql)

    def find_done_homework_id(self, student_id):
        sql = 'SELECT homework_id FROM `homework_student_record`  WHERE student_id = "{0}"' \
              'GROUP BY homework_id '.format(student_id)
        return self.execute_sql_return_result(sql)

    def find_done_homework_overview(self, student_id, homework_id):
        sql = 'SELECT * FROM homework_student_overview WHERE student_id ="{0}" and homework_id = "{1}"'\
            .format(student_id, homework_id)
        return self.execute_sql_return_result(sql)

    def find_done_homework_record(self, student_id, homework_id):
        """查询某一条作业记录，进行复制"""
        sql = 'SELECT * FROM `homework_student_record` WHERE student_id = "{0}" and homework_id = "{1}"'\
            .format(student_id,homework_id)
        return self.execute_sql_return_result(sql)

    def find_done_record_id_by_bank_id(self, homework_id, bank_id):
        sql = 'SELECT `id` FROM `homework_student_record` WHERE homework_id = "{0}" and testbank_id = "{1}"'\
            .format(homework_id, bank_id)
        return self.execute_sql_return_result(sql)

    def find_done_wrong_by_record_id(self, record_id, student_id):
        sql = 'SELECT * FROM `homework_student_wrong` WHERE record_id ="{0}" and student_id = "{1}"'\
            .format(record_id, student_id)
        return self.execute_sql_return_result(sql)

    def add_homework_overview(self, q, homework_id, student_id, testbank_id):
        """在overview表中添加复制习题信息"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "INSERT INTO `homework_student_overview`(homework_id,student_id,testbank_id,right_ids,do_count," \
              "completion_rate,like_count,created_at,updated_at) VALUES('{0}','{1}','{2}','{3}','{4}','{5}'," \
              "'{6}','{7}','{8}')".format(homework_id, student_id, testbank_id, q[4], q[5], q[6], q[8], now, now)
        self.execute_sql_only(sql)

    def add_homework_record(self, q, homework_id, student_id, testbank_id):
        """添加 作业记录"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        start_time = (datetime.datetime.now() + datetime.timedelta(minutes=-2)).strftime("%Y-%m-%d %H:%M:%S")
        start_date = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        end_time = (start_date + datetime.timedelta(minutes=-1.5)).strftime("%Y-%m-%d %H:%M:%S")
        sql = 'INSERT INTO homework_student_record(related_record_id,homework_id,student_id,testbank_id,' \
              'testbank_item_count,wrong_count,wrong_ids,right_count,right_ids,do_again,start_time,end_time,spend_time,' \
              'created_at,updated_at) VALUES("{0}","{1}","{2}","{3}","{4}","{5}","{6}","{7}","{8}","{9}","{10}",' \
              '"{11}","{12}","{13}","{14}")'.format(q[1], homework_id, student_id, testbank_id, q[5], q[6], q[7], q[8],
                                                    q[9], q[10], start_time, end_time, '00:00:30', now, now)
        self.execute_sql_only(sql)

    def add_homework_wrong(self, q, homework_id, student_id, record_id, testbank_id):
        """在错题表中添加复制习题的错题信息"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "INSERT INTO homework_student_wrong(homework_id, student_id, testbank_id, testbank_type_id, record_id," \
              "testbank_item_id, exercise_name, answer, do_again, wrong_again_sum, is_done, created_at, updated_at) VALUES" \
              "('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}')"\
            .format(homework_id, student_id, testbank_id, q[4], record_id, q[6], q[7], q[8], q[9], q[10], q[11], now, now)
        self.execute_sql_only(sql)

# 删除作业记录
    def delete_homework_wrong(self, homework_id):
        sql = 'DELETE FROM `homework_student_wrong` WHERE  homework_id = "{}"'.format(homework_id)
        self.execute_sql_only(sql)

    def delete_homework_overview(self, homework_id):
        sql = 'DELETE FROM `homework_student_overview` WHERE  homework_id = "{}"'.format(homework_id)
        self.execute_sql_only(sql)

    def delete_homework_record(self, homework_id):
        sql = 'DELETE FROM `homework_student_record` WHERE  homework_id = "{}"'.format(homework_id)
        self.execute_sql_only(sql)

    def delete_testbank_quote_record(self, object_id):
        sql = 'DELETE FROM `testbank_quote_record` WHERE  object_name = "{}"'.format(object_id)
        self.execute_sql_only(sql)

    def delete_exercise_overview(self, homework_id):
        sql = 'DELETE  FROM `teacher_exercise_overview` WHERE exercise_id = "{}"'.format(homework_id)
        self.execute_sql_only(sql)

    def delete_class_stu_homework(self, homework_id):
        sql = 'DELETE  FROM `vanclass_student_homework` WHERE homework_id = "{}"'.format(homework_id)
        self.execute_sql_only(sql)

    def delete_homework(self, homework_id):
        sql = 'DELETE  FROM `homework` WHERE `id` = "{}"'.format(homework_id)
        self.execute_sql_only(sql)

    def delete_quote_bank(self, quoted_id):
        sql = 'DELETE  FROM `user_quoted_testbank` WHERE `id` = "{}"'.format(quoted_id)
        self.execute_sql_only(sql)

# 更改作业完成度
    def find_all_homework_id(self, class_id):
        """查询 所有 作业id"""
        sql = 'SELECT `id` FROM `vanclass_student_homework` WHERE vanclass_id = "%s"' % (class_id,)
        return self.execute_sql_return_result(sql)

    def update_complete_rate(self, homework_id):
        """根据id 更改作业的完成度信息"""
        sql = 'UPDATE  `vanclass_student_homework` SET is_finish = "1", completion_rate = "100" ,is_finish_for_teacher="1",' \
              'completion_rate_for_teacher= "100"  WHERE `homework_id` = "{}"'.format(homework_id, )
        self.execute_sql_only(sql)


# ===================================== 试卷部分 ============================================================

    def find_quote_entity(self, quote_id):
        """查询某一习题的所有信息"""
        sql = 'SELECT * FROM `user_quoted_testbank_entity` WHERE quoted_testbank_id = "{}"'.format(quote_id,)
        return self.execute_sql_return_result(sql)

    def add_quote_bank_entity(self, testbank_id, q):
        """将复制的testbank添加至 entity表"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if q[2] is not None:
            extra_value = pymysql.escape_string(q[2])
        else:
            extra_value = ''

        if q[3] is not None:
            item_value = pymysql.escape_string(q[3])
        else:
            item_value = ''

        sql = 'INSERT INTO `user_quoted_testbank_entity`(quoted_testbank_id,testbank_extra_value,' \
              'testbank_item_value,created_at,updated_at) ' \
              "VALUES('{0}','{1}','{2}','{3}','{4}')".format(testbank_id, extra_value, item_value, now, now)
        self.execute_sql_only(sql)

    def find_testbank_item_count(self, testbank_name):
        """查询题型中大题个数"""
        sql = "SELECT `item_count` FROM `user_quoted_testbank` WHERE name like '%{}%' ".format(testbank_name, )
        return self.execute_sql_return_result(sql)

    def find_exam_name(self, quote_name):
        sql = " SELECT name FROM test_quotation WHERE `name` like '{}%'".format(quote_name)
        return self.execute_sql_return_result(sql)

    def find_exam_info(self, name):
        """查询试卷id"""
        sql = 'SELECT * FROM `test` WHERE `name` = "%s"' % (name,)
        return self.execute_sql_return_result(sql)

    def add_test(self, name, bank_array, item_count, account_id):
        """添加试卷"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = 'insert into `test` (`name`, keyword, description,hot_words_id, `section_ids`, `item_count`,`total_score`,' \
              ' `exam_time`,`limit_time`, `is_limited`, favorite_num,price,special_price,`account_id`, `status`,' \
              'is_public, `is_active`, `is_recommend`, is_top,es_indexed,`created_at`,`updated_at`) values(' \
              '"{0}","{1}","{2}","{3}","{4}","{5}","{6}","{7}","{8}","{9}","{10}","{11}","{12}","{13}","{14}","{15}",' \
              '"{16}","{17}","{18}","{19}","{20}","{21}")'.format(name, "", "", 0, bank_array, item_count, 100, 60,
                                                                  0, 0, 0, 0, 0, account_id, 1, 0, 1, 1, 0, 1, now, now)
        self.execute_sql_only(sql)

    def add_test_quotation(self, q, name, origin_quotation_id, class_id):
        """复制大题到库中"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "INSERT INTO `test_quotation`(`name`, `same_name_index`, `keyword`, `description`, `test_id`, " \
              "`origin_quotation_id`, `section_ids`, `item_count`, `total_score`, `exam_time`, `is_limited`, " \
              "`favorite_num`, `account_id`, `public_vanclass_ids`, `status`,`is_visible`,`created_at`, `updated_at`)" \
              "values('{0}','{1}','{2}','{3}','{4}',{5},'{6}','{7}', '{8}','{9}','{10}','{11}','{12}','{13}','{14}'" \
              ",'{15}','{16}','{17}')".format(name, 0, q[2], q[3], q[0], origin_quotation_id, q[5], q[6], q[9], q[10],
                                              q[12], q[13], q[17], class_id, q[18], 0, now, now)
        self.execute_sql_only(sql)

    def find_test_quotation_ids(self, name):
        """查询已布置试卷的id"""
        sql = 'SELECT `id` FROM `test_quotation` WHERE `name` like "%{}%"'.format(name, )
        return self.execute_sql_return_result(sql)

    def add_quoted_map(self, object_type, object_id, testbank_id, score, sort_by):
        """在map表中添加复制习题信息"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = 'insert into `test_quoted_testbank_map` (object_type, object_id, quoted_testbank_id, score, sort_by,' \
              ' updated_at, created_at) values("{0}","{1}","{2}","{3}","{4}","{5}","{6}")'. \
            format(object_type, object_id, testbank_id, score, sort_by, now, now)
        self.execute_sql_only(sql)

    def add_exam_student(self, class_id, student_id, quotation_id, teacher_id):
        """给学生添加试卷"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = 'insert into `exam_student` ( `vanclass_id`, `student_id`, `quotation_id`, `teacher_id`, ' \
              '`created_at`, `updated_at`) values("{0}","{1}","{2}","{3}","{4}","{5}")' \
            .format(class_id, student_id, quotation_id, teacher_id, now, now)
        self.execute_sql_only(sql)

    def find_done_quote_record(self, student_id):
        """查找做过习题的id"""
        sql = 'SELECT * FROM `exam_student_record` WHERE  student_id= "{}" AND right_count != 0'.format(student_id, )
        return self.execute_sql_return_result(sql)

    def update_done_exercise_score(self, score, record_id):
        """查询做过习题的做题记录数据"""
        sql = 'UPDATE `exam_student_record` SET score="{0}" where `id` = "{1}"'.format(score, record_id, )
        self.execute_sql_only(sql)

    def find_no_done_item_ids(self, quotation_id, section_id):
        """查询没做过的习题的错题 id和正确习题的id"""
        sql = 'SELECT wrong_ids,right_ids,right_count FROM `exam_student_record` WHERE quotation_id = "{0}" AND  ' \
              'section_id="{1}"'.format(quotation_id, section_id)
        return self.execute_sql_return_result(sql)

    def update_no_exercise_record(self, score, wrong_count, wrong_ids, right_count, right_ids, quotation_id,
                                  section_id):
        """更改没有做过习题的record数据，更改错题和正确题目数量以及他们的id"""
        sql = 'UPDATE `exam_student_record` SET score="{0}", wrong_count= "{1}", wrong_ids = "{2}",right_count = "{3}",' \
              'right_ids = "{4}" WHERE quotation_id = "{5}" and section_id = "{6}"' \
            .format(score, wrong_count, wrong_ids, right_count, right_ids, quotation_id, section_id)
        self.execute_sql_only(sql)

    def find_done_wrong_by_section_id(self, item_id):
        """查找已经做过习题的错题问题及答案"""
        sql = 'SELECT exercise_name, answer FROM `exam_student_wrong` WHERE item_id = "%s"' % (item_id,)
        return self.execute_sql_return_result(sql)

    def update_no_do_wrong(self, exercise_name, answer, item_id):
        """更改没有做过习题的错题记录"""
        sql = 'UPDATE `exam_student_wrong` SET exercise_name = "{0}", answer ="{1}" WHERE item_id = "{2}"' \
            .format(exercise_name, answer, item_id)
        self.execute_sql_only(sql)

    def delete_no_do_right_answer(self, item_id):
        """在错题表中删除掉 未做习题正确题目对应的数据"""
        sql = 'DELETE FROM `exam_student_wrong` WHERE item_id  = "{0}" '.format(item_id, )
        self.execute_sql_only(sql)

    def find_testbank_score(self, quotation_id):
        """查询每一题大题的分数"""
        sql = "SELECT score FROM `exam_student_record` WHERE quotation_id = '%s'" % (quotation_id,)
        return self.execute_sql_return_result(sql)

    def update_exam_student_score(self, score, quotation_id, student_id):
        """更改试卷的最终分数"""
        sql = 'UPDATE `exam_student` SET score = "{0}", spend_time = "{1}"  WHERE quotation_id = "{2}" ' \
              'and student_id ="{3}"'.format(score, "00:00:30", quotation_id, student_id)
        return self.execute_sql_only(sql)

    def find_complete_student_id(self, quotation_id):
        """查询已完成试卷学生id"""
        sql = 'SELECT student_id FROM `exam_student_record` WHERE quotation_id = "{}" GROUP BY student_id' \
            .format(quotation_id, )
        return self.execute_sql_return_result(sql)

    def find_one_stu_exam_record(self, quotation_id, student_id):
        """查询已完成试卷学生下的一条试卷记录"""
        sql = 'SELECT * FROM `exam_student_record` WHERE quotation_id = "{0}" and student_id = "{1}"' \
            .format(quotation_id, student_id)
        return self.execute_sql_return_result(sql)

    def find_one_stu_exam_wrong(self, quote_id):
        """查询已完成试卷学生下的一条错题记录"""
        sql = 'SELECT * FROM  `exam_student_wrong` WHERE quotation_id = "{0}"'.format(quote_id)
        return self.execute_sql_return_result(sql)

    def add_exam_student_record(self, q, quotation_id, student_id):
        """添加学生试卷记录"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = 'INSERT INTO `exam_student_record`(quotation_id, student_id,section_id, item_count, score, wrong_count,' \
              ' wrong_ids, right_count, right_ids, start_time, end_time,spend_time, created_at, updated_at) ' \
              'VALUES("{0}","{1}","{2}","{3}","{4}","{5}","{6}","{7}","{8}","{9}","{10}","{11}","{12}","{13}")' \
            .format(quotation_id, student_id, q[3], q[4], q[5], q[6], q[7], q[8], q[9], q[10], q[11], q[12], now, now)
        self.execute_sql_only(sql)

    def add_exam_student_wrong(self, q, quotation_id, student_id, record_id):
        """添加学生试卷错题记录"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = 'INSERT INTO `exam_student_wrong`(quotation_id,student_id,section_id,record_id, item_id,exercise_name,' \
              'answer, created_at, updated_at) VALUES("{0}","{1}","{2}","{3}","{4}","{5}","{6}","{7}","{8}")' \
            .format(quotation_id, student_id, q[3], record_id, q[5], q[6], q[7], now, now)
        self.execute_sql_only(sql)

    def find_done_quote_id(self, student_id):
        sql = 'SELECT quotation_id FROM `exam_student_record`  WHERE student_id = "{0}"' \
              'GROUP BY quotation_id '.format(student_id)
        return self.execute_sql_return_result(sql)

    # 删除试卷记录
    def delete_exam_wrong(self, quotation_id):
        sql = 'DELETE FROM `exam_student_wrong`WHERE quotation_id  = "{}" '.format(quotation_id)
        self.execute_sql_only(sql)

    def delete_exam_record(self, quotation_id):
        sql = 'DELETE FROM `exam_student_record` WHERE quotation_id  = "{}" '.format(quotation_id)
        self.execute_sql_only(sql)

    def delete_quote_map(self, testbank_id):
        sql = 'DELETE FROM `test_quoted_testbank_map` WHERE quoted_testbank_id  = "{}" '.format(testbank_id)
        self.execute_sql_only(sql)

    def delete_quote_entity(self, testbank_id):
        sql = 'DELETE FROM `user_quoted_testbank_entity` WHERE quoted_testbank_id = "{}"'.format(testbank_id)
        self.execute_sql_only(sql)

    def delete_student_exam(self, quotation_id):
        sql = 'DELETE FROM `exam_student` WHERE quotation_id  = "{}" '.format(quotation_id)
        self.execute_sql_only(sql)

    def delete_test_quotation(self, quotation_id):
        sql = 'DELETE FROM `test_quotation` WHERE `id` = "{}" '.format(quotation_id)
        self.execute_sql_only(sql)

    def delete_test(self, test_name):
        sql = 'DELETE FROM `test` WHERE `name` like "%{}%" '.format(test_name)
        self.execute_sql_only(sql)

# ===================================== 单词部分 ============================================================
    def find_label_id(self):
        """查询表数据下所有有单词的标签id"""
        sql = 'SELECT id FROM label WHERE id in (SELECT label_id FROM wordbank_label_overview)'
        return self.execute_sql_return_result(sql)

    def fins_label_name_parent(self, label_id):
        """查询标签的名称以及父级标签的id"""
        sql = "SELECT `name`,parent_id FROM label WHERE id = '{}'".format(label_id)
        return self.execute_sql_return_result(sql)

    def find_sys_label_by_name(self, name):
        """根据名称查询系统标签id"""
        sql = 'SELECT `id` FROM  label WHERE label_type_id = "4" AND name = "{}"'.format(name)
        return self.execute_sql_return_result(sql)

    def find_wordbank_label_by_sys_label(self, label_id):
        """根据名称得来的系统标签id查询对应的
            wordbank_label_id和word_homework_id
        """
        sql = 'SELECT wordbank_label_id, word_homework_id FROM label_student_wordbank_map WHERE student_label_id = "{}"' \
            .format(label_id)
        return self.execute_sql_return_result(sql)

    def find_sys_wordbank_id(self):
        """查询所有的系统标签"""
        sql = 'SELECT `wordbank_label_id` FROM  label_student_wordbank_map where type="system"'
        return self.execute_sql_return_result(sql)

    def find_sys_homework_id_by_wordbank(self, wordbank_label_id):
        """根据系统标签查到对应的作业id"""
        sql = 'SELECT `word_homework_id` FROM  `label_student_wordbank_map` WHERE wordbank_label_id = "{}"'.format(
            wordbank_label_id)
        return self.execute_sql_return_result(sql)

    def find_data_studnet_label_id(self, student_id):
        """查询学生数据表下key为student_label_id的数据"""
        sql = "SELECT * FROM `user_student_data` WHERE `key` = 'student_label_id' and " \
              " user_account_id ='{}'".format(student_id)
        return self.execute_sql_return_result(sql)

    def add_user_student_data(self, student_id, key, value):
        """在学生数据表中添加一条key为student_label_id的数据"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = 'INSERT INTO `user_student_data`(user_account_id, `key`,key_id, `value`, created_at, updated_at) ' \
              'VALUES("{0}","{1}", 0,"{2}","{3}","{4}")'.format(student_id, key, value, now, now)
        self.execute_sql_only(sql)

    def add_word_homework(self, name, teacher_id, class_id):
        """添加单词作业（老师布置的）"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "INSERT INTO word_homework(name, teacher_id, public_vanclass_ids, status,created_at, updated_at) " \
              "VALUES('{0}','{1}','{2}','{3}','{4}','{5}')".format(name, teacher_id, class_id, '1', now, now)
        self.execute_sql_only(sql)

    def add_word_homework_student(self, vanclass_id, student_id, word_homework_id, label_ids, is_system):
        """添加单词作业对应的单词表数据"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "INSERT INTO `word_homework_student`(vanclass_id,student_id, word_homework_id, label_ids, is_system, " \
              "created_at, updated_at) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}')" \
            .format(vanclass_id, student_id, word_homework_id, label_ids, is_system, now, now)
        self.execute_sql_only(sql)

    def add_word_fluency(self, studnet_id, wordbank_id, word_homework_id, is_system, label_ids, level):
        """将单词逐个添加到add_word_fluency表中"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "INSERT INTO `word_student_fluency`(student_id,wordbank_id,word_homework_id,is_system, label_ids, " \
              "fluency_level, last_finish_at, created_at, updated_at) VALUES('{0}','{1}','{2}','{3}','{4}','{5}','{6}'," \
              "'{7}','{8}')".format(studnet_id, wordbank_id, word_homework_id, is_system, label_ids, level, now, now,
                                    now)
        self.execute_sql_only(sql)

    def find_student_have_label_ids(self, student_id):
        sql = 'SELECT label_ids FROM word_student_fluency WHERE student_id = "{}" GROUP BY label_ids'.format(student_id)
        return self.execute_sql_return_result(sql)

    def find_student_fluency_id(self, student_id, label_id):
        sql = "SELECT id, wordbank_id FROM `word_student_fluency` WHERE student_id = '{0}' AND label_ids = '{1}'" \
            .format(student_id, label_id)
        return self.execute_sql_return_result(sql)

    def find_fluency_id_by_student_id(self, student_id):
        sql = "SELECT id FROM `word_student_fluency` WHERE student_id = '{0}'".format(student_id, student_id)
        return self.execute_sql_return_result(sql)

    def add_word_homework_student_record(self, student_id, wordbank_ids, fluency_ids, word_count):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "INSERT INTO `word_homework_student_record`(student_id,  wordbank_ids, fluency_ids, wordbank_count, " \
              "wrong_count, wrong_fluency_ids, spend_time, created_at, updated_at) " \
              "VALUES('{0}','{1}','{2}','{3}',0,'','00:08:00','{4}','{5}')"\
            .format(student_id, wordbank_ids, fluency_ids, word_count, now, now)
        self.execute_sql_only(sql)

    def add_word_student_fluency_record(self, student_fluency_id):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = 'INSERT INTO `word_student_fluency_record`(student_fluency_id, fluency_level,created_at,updated_at) ' \
              'VALUES("{0}",1,"{1}","{2}")'.format(student_fluency_id, now, now)
        self.execute_sql_only(sql)

    def find_word_homework_id(self, teacher_id, name):
        """根据作业名称查询作业id"""
        sql = 'SELECT `id` FROM word_homework WHERE teacher_id = "{0}" and `name` = "{1}"'.format(teacher_id, name)
        return self.execute_sql_return_result(sql)

    def find_word_by_label(self, label_id):
        """查询标签下所有单词"""
        sql = "SELECT `content` FROM `wordbank_label_overview` WHERE label_id ='{}'".format(label_id)
        return self.execute_sql_return_result(sql)

    def delete_before_created_homework(self, class_id):
        sql = 'DELETE FROM word_homework WHERE public_vanclass_ids = "{}"'.format(class_id)
        self.execute_sql_only(sql)

    def delete_student_word_fluency(self, student_id):
        """删除用户下所有的单词"""
        sql = 'DELETE  FROM  `word_student_fluency` WHERE student_id = "{}"'.format(student_id)
        self.execute_sql_only(sql)

    def delete_student_word_homework(self, student_id):
        """删除学生对应的作业"""
        sql = 'DELETE  FROM  `word_homework_student` WHERE student_id = "{}"'.format(student_id)
        self.execute_sql_only(sql)

    def delete_student_word_data(self, student_id, key):
        """删除key为student_label_id的数据"""
        sql = 'DELETE  FROM  `user_student_data`  WHERE user_account_id = "{0}" AND `key` = "{1}"'.format(student_id, key)
        self.execute_sql_only(sql)

    def delete_student_fluency_record(self, student_fluency_id):
        sql = 'DELETE FROM `word_student_fluency_record` WHERE student_fluency_id = "{}"'.format(student_fluency_id)
        self.execute_sql_only(sql)

    def delete_word_homework_record(self, student_id):
        sql = 'DELETE FROM `word_homework_student_record` WHERE student_id = "{}"'.format(student_id)
        self.execute_sql_only(sql)

# ===================================== 星星积分部分 ============================================================
    def add_student_score_star(self, student_id, key, value):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = 'INSERT INTO `user_student_data`(user_account_id, `key`,key_id, `value`, created_at, updated_at) ' \
              'VALUES("{0}","{1}", 0,"{2}", "{3}","{4}")'.format(student_id, key, value, now, now)
        self.execute_sql_only(sql)