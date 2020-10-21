text1 = input("Enter text 1")
text2 = input("Enter text 2")


for x in text1:
        if x in text2:
                pass
        else:
                print("Not anagram")
                break
else:
        print("Anagram")