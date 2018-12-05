import random
from conf import config
from stress_point.common import CommonAction



class StartScoreAction:
    def __init__(self):
        self.common = CommonAction()

    def add_star_to_student(self):
        student_list = self.common.get_class_student()
        for j in range(5):
            for i in range(len(student_list)):
                value = random.randint(10, 200)
                self.common.add_student_score_star(student_list[i],  'star', value)
                self.common.add_student_score_star(student_list[i], 'score', value)

    def delete_student_star_score(self):
        pass


if __name__ == '__main__':
    score = StartScoreAction()
    score.add_star_to_student()
    score.common.close_db()