from extractors import PoipikuExtractor

extr = PoipikuExtractor()
extr.load_cookies(".secret/cookies.txt")
for i in extr.extract("https://poipiku.com/11581691/12486332.html"):
	print(i)