import json

filenames = ['user', 'sent', 'msg_queue']
for filename in filenames:
    for i in range(1, 4):
        with open(filename + str(i) + '.json', 'w') as f:
            json.dump({}, f)
