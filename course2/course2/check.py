def check_income_list(income, outcome, average_income, total_outcome, balance):
    
    
    names = [
        'income',
        'average_income',
        'total_outcome',
        'balance',
    ]
    
    answers = [
        income,
        average_income,
        total_outcome,
        balance,
    ]
    
    correct_answers = [
        [2,2,2,3,3,4],
        sum(income) / len(income),
        sum(outcome),
        sum(income) - sum(outcome),
    ]
    
    for i, name, a, ca in zip(range(len(answers)), names, answers, correct_answers):
        print('第', i+1, '題', end=" ")
        if a == ca:
            print('答對了！')
        else:
            print('答錯囉！', name, '應該是', ca, '而不是', a)

def normal_check(answer, correct_answer):
    correct = (answer == correct_answer)
    if correct:
        print('恭喜你！答對了～')
    else:
        print('當中有一些小錯誤，再檢查一下吧！')