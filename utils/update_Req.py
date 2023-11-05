from pur import update_requirements

print([x[0]['message'] for x in update_requirements(input_file='requirements.txt').values()])

print(open('requirements.txt').read())