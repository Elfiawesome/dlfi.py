from extractors import PoipikuExtractor

extr = PoipikuExtractor()
extr.load_cookies(".secret/cookies.txt")
LINK = "https://poipiku.com/294397/"
# LINK = "https://poipiku.com/11581691/"

count = 0

for i in extr.extract(LINK):
	count += 1
	print(f"{count}: {i.files}")
# 35