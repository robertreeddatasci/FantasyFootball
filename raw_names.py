raw_names = '''
Jacory Croskey-Merritt
Ollie Gordon II
Dyami Brown
Dont'e Thornton Jr.
Tory Horton
Sean Tucker
Tyler Shough
Isaiah Davis
Shedeur Sanders
Jaylin Lane
'''
# Split lines, add quotes and comma
formatted_names = [f'"{name.strip()}",' for name in raw_names.strip().split("\n")]
print("\n".join(formatted_names))
