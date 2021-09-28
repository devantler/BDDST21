from pyspark import SparkConf, SparkContext
import locale
locale.getdefaultlocale()
locale.getpreferredencoding()

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# Read in negative and positive words list
positiveWords = ['absolutely','accepted','acclaimed','accomplish','accomplishment','achievement','action','active','admire','adorable','adventure','affirmative','affluent','agree','agreeable','amazing','angelic','appealing','approve','aptitude','attractive','awesome','beaming','beautiful','believe','beneficial','bliss','bountiful','bounty','brave','bravo','brilliant','bubbly','calm','celebrated','certain','champ','champion','charming','cheery','choice','classic','classical','clean','commend','composed','congratulation','constant','cool','courageous','creative','cute','dazzling','delight','delightful','distinguished','divine','earnest','easy','ecstatic','effective','effervescent','efficient','effortless','electrifying','elegant','enchanting','encouraging','endorsed','energetic','energized','engaging','enthusiastic','essential','esteemed','ethical','excellent','exciting','exquisite','fabulous','fair','familiar','famous','fantastic','favorable','fetching','fine','fitting','flourishing','fortunate','free','fresh','friendly','fun','funny','generous','genius','genuine','giving','glamorous','glowing','good','gorgeous','graceful','great','green','grin','growing','handsome','happy','harmonious','healing','healthy','hearty','heavenly','honest','honorable','honored','hug','idea','ideal','imaginative','imagine','impressive','independent','innovate','innovative','instant','instantaneous','instinctive','intellectual','intelligent','intuitive','inventive','jovial','joy','jubilant','keen','kind','knowing','knowledgeable','laugh','learned','legendary','light','lively','lovely','lucid','lucky','luminous','marvelous','masterful','meaningful','merit','meritorious','miraculous','motivating','moving','natural','nice','novel','now','nurturing','nutritious','okay','one','one-hundred percent','open','optimistic','paradise','perfect','phenomenal','pleasant','pleasurable','plentiful','poised','polished','popular','positive','powerful','prepared','pretty','principled','productive','progress','prominent','protected','proud','quality','quick','quiet','ready','reassuring','refined','refreshing','rejoice','reliable','remarkable','resounding','respected','restored','reward','rewarding','right','robust','safe','satisfactory','secure','seemly','simple','skilled','skillful','smile','soulful','sparkling','special','spirited','spiritual','stirring','stunning','stupendous','success','successful','sunny','super','superb','supporting','surprising','terrific','thorough','thrilling','thriving','tops','tranquil','transformative','transforming','trusting','truthful','unreal','unwavering','up','upbeat','upright','upstanding','valued','vibrant','victorious','victory','vigorous','virtuous','vital','vivacious','wealthy','welcome','well','whole','wholesome','willing','wonderful','wondrous','worthy','wow','yes','yummy','zeal','zealous']
negativeWords = ['abysmal','adverse','alarming','angry','annoy','anxious','apathy','appalling','atrocious','awful','bad','banal','barbed','belligerent','bemoan','beneath','boring','broken','callous','can\'t','clumsy','coarse','cold','cold-hearted','collapse','confused','contradictory','contrary','corrosive','corrupt','crazy','creepy','criminal','cruel','cry','cutting','damage','damaging','dastardly','dead','decaying','deformed','deny','deplorable','depressed','deprived','despicable','detrimental','dirty','disease','disgusting','disheveled','dishonest','dishonorable','dismal','distress','don\'t','dreadful','dreary','enraged','eroding','evil','fail','faulty','fear','feeble','fight','filthy','foul','frighten','frightful','gawky','ghastly','grave','greed','grim','grimace','gross','grotesque','gruesome','guilty','haggard','hard','hard-hearted','harmful','hate','hideous','homely','horrendous','horrible','hostile','hurt','hurtful','icky','ignorant','ignore','ill','immature','imperfect','impossible','inane','inelegant','infernal','injure','injurious','insane','insidious','insipid','jealous','junky','lose','lousy','lumpy','malicious','mean','menacing','messy','misshapen','missing','misunderstood','moan','moldy','monstrous','naive','nasty','naughty','negate','negative','never','no','nobody','nondescript','nonsense','not','noxious','objectionable','odious','offensive','old','oppressive','pain','perturb','pessimistic','petty','plain','poisonous','poor','prejudice','questionable','quirky','quit','reject','renege','repellant','reptilian','repugnant','repulsive','revenge','revolting','rocky','rotten','rude','ruthless','sad','savage','scare','scary','scream','severe','shocking','shoddy','sick','sickening','sinister','slimy','smelly','sobbing','sorry','spiteful','sticky','stinky','stormy','stressful','stuck','stupid','substandard','suspect','suspicious','tense','terrible','terrifying','threatening','ugly','undermine','unfair','unfavorable','unhappy','unhealthy','unjust','unlucky','unpleasant','unsatisfactory','unsightly','untoward','unwanted','unwelcome','unwholesome','unwieldy','unwise','upset','vice','vicious','vile','villainous','vindictive','wary','weary','wicked','woeful','worthless','wound','yell','yucky','zero']
    
print('Positive word count: ' + str(len(positiveWords)) + ', negative word count: ' + str(len(negativeWords)))

# Limit cores to 1, and tell each executor to use one core = only one executor is used by Spark
conf = SparkConf().set('spark.executor.cores', 1).set('spark.cores.max',1).set('spark.executor.memory', '1g')
sc = SparkContext(master='spark://spark-master:7077', appName='myAppName', conf=conf)

print(bcolors.WARNING+"EXERCISE: TOP WORDS"+bcolors.ENDC)
select_words = lambda s : s[1] > 400
files = "hdfs://namenode:9000/txt/"
# Read in all files in the directory
txtFiles = sc.wholeTextFiles(files, 20)
# Take the content of the files and split them
all_word = txtFiles.flatMap(lambda s: s[1].split())
# Change from list of words to list of (word, 1)
word_map = all_word.map(lambda s: (s, 1))
# Merge values with equal keys
word_reduce = word_map.reduceByKey(lambda s, t: s+t)
# Filter using the defined lambda and sort by value
top_words = word_reduce.filter(select_words).sortBy(lambda s: s[1])
# Collect to a Python list and print
print(bcolors.OKGREEN + str(top_words.collect()) + bcolors.ENDC)

print(bcolors.WARNING+"EXERCISE: SENTIMENT POSITIVE/NEGATIVE WORDS"+ bcolors.ENDC)
file = "hdfs://namenode:9000/txt/alice.txt"
# Read in alice.txt
txtFile = sc.textFile(file)
# Take the content of the file and split them
all_word = txtFile.flatMap(lambda s: s.split())
# Change from list of words to list of (word, 1)
word_map = all_word.map(lambda s: (s, 1))
# Merge values with equal keys
word_reduce = word_map.reduceByKey(lambda s, t: s+t)
# Filter using the defined lambda and sort by value
positive_words = word_reduce.filter(lambda s: s[0] in positiveWords)
# Filter using the defined lambda and sort by value
negative_words = word_reduce.filter(lambda s: s[0] in negativeWords)
# Add together all positive word counts
positiveWordCount = positive_words.map(lambda s: int(s[1])).sum()
# Add together all negative word counts
negativeWordCount = negative_words.map(lambda s: int(s[1])).sum()
# Collect to a Python list and print
print(bcolors.OKGREEN + "Positive word count: " + str(positiveWordCount) + ", Negative word count: " + str(negativeWordCount)  + bcolors.ENDC)

print(bcolors.WARNING+"EXERCISE: SENTIMENT POSITIVE/NEGATIVE SENTENCES"+ bcolors.ENDC)
file = "hdfs://namenode:9000/txt/alice.txt"
# Read in alice.txt
txtFile = sc.textFile(file)
# Take the content of the file and split them to sentences
all_sentences = txtFile.flatMap(lambda s: s.split("."))
sentiment_map = all_sentences.map(lambda s: (s, 0))
 
# Count occurences of negative and positive words
final_map0 = sentiment_map.map(lambda s: (s[0],s[1] + sum(s[0].replace(',', '').split().count(i) for i in positiveWords)))
final_map1 = final_map0 .map(lambda s: (s[0],s[1] - sum(s[0].replace(',', '').split().count(i) for i in negativeWords)))

print(bcolors.OKGREEN + str(final_map1.filter(lambda s: s[1] != 0).sortBy(lambda s: s[1]).collect()) + bcolors.ENDC)