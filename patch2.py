import sys

def patch(file, target, repl):
    with open(file, 'r') as f:
        c = f.read()
    if target in c:
        with open(file, 'w') as f:
            f.write(c.replace(target, repl))
        print(f'Patched {file}')
    else:
        print(f'Target not found in {file}')

patch('processor/daily_brief.py', 
      'topic_clauses = [{"topic": {"$eq": interest}} for interest in valid_interests]', 
      'topic_clauses = [{"topics": {"$contains": interest}} for interest in valid_interests]')

patch('api/query_engine.py', 
      'topic_clauses = [{"topic": {"$eq": interest}} for interest in interests]', 
      'topic_clauses = [{"topics": {"$contains": interest}} for interest in interests]')
