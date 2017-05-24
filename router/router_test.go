package router

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xuther/go-message-router/common"
	"github.com/xuther/go-message-router/publisher"
	"github.com/xuther/go-message-router/subscriber"
)

func TestRouter(t *testing.T) {

	routingGuide := make(map[string][]string)

	routingGuide["a"] = []string{"b", "c"}
	routingGuide["b"] = []string{"a"}
	routingGuide["c"] = []string{"d"}

	pub, err := publisher.NewPublisher("60000", 1000, 10)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	go pub.Listen()
	time.Sleep(1 * time.Second) //give the os time to bind teh tcp port

	var wg sync.WaitGroup
	wg.Add(3)

	r := Router{}

	err = r.Start(routingGuide, wg, 1000, []string{"localhost:60000"}, 0, 0, "60005")
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	sub, err := subscriber.NewSubscriber(3000)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	sub.Subscribe("localhost:60005", []string{"a", "b", "c", "d"})

	time.Sleep(1 * time.Second) //give the os time to bind teh tcp port

	headera := [24]byte{}
	copy(headera[:], "a")

	headerb := [24]byte{}
	copy(headerb[:], "b")

	headerc := [24]byte{}
	copy(headerc[:], "c")

	pub.Write(common.Message{headera, []byte("a->b|a->c")})

	pub.Write(common.Message{headerb, []byte("b -> a")})

	pub.Write(common.Message{headerc, []byte("c -> d")})

	a := sub.Read()
	fmt.Printf("|")
	b := sub.Read()
	fmt.Printf("|")
	c := sub.Read()
	fmt.Printf("|")
	d := sub.Read()
	fmt.Printf("|\n")

	log.Printf("%s - %s", a.MessageHeader, a.MessageBody)
	log.Printf("%s - %s", b.MessageHeader, b.MessageBody)
	log.Printf("%s - %s", c.MessageHeader, c.MessageBody)
	log.Printf("%s - %s", d.MessageHeader, d.MessageBody)
}

//test remote subscription based on sending a subscribe packet
func TestSocketSubscription(t *testing.T) {

	//Start setup

	routingGuide := make(map[string][]string)

	routingGuide["a"] = []string{"b"}
	routingGuide["c"] = []string{"d", "b"}
	routingGuide["d"] = []string{"b"}

	pub, err := publisher.NewPublisher("60010", 1000, 10)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	go pub.Listen()
	time.Sleep(1 * time.Second) //give the os time to bind the tcp port

	var wg1 sync.WaitGroup
	wg1.Add(3)

	r1 := Router{}

	err = r1.Start(routingGuide, wg1, 1000, []string{"localhost:60012"}, 10, time.Second*5, "60011")
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	var wg2 sync.WaitGroup
	wg2.Add(3)

	time.Sleep(1 * time.Second) //give the os time to bind the tcp port

	r2 := Router{}

	err = r2.Start(routingGuide, wg2, 1000, []string{"localhost:60011", "localhost:60010"}, 10, time.Second*1, "60012")
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	sub, err := subscriber.NewSubscriber(3000)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	sub.Subscribe("localhost:60011", []string{"a", "b", "c"})
	sub1, err := subscriber.NewSubscriber(3000)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	sub1.Subscribe("localhost:60012", []string{"a", "b", "c"})
	time.Sleep(1 * time.Second) //give the os time to bind the tcp port

	headera := [24]byte{}
	copy(headera[:], "a")

	headerb := [24]byte{}
	copy(headerb[:], "b")

	headerc := [24]byte{}
	copy(headerc[:], "c")

	headerd := [24]byte{}
	copy(headerd[:], "d")

	//End Setup
	pub.Write(common.Message{MessageHeader: headera, MessageBody: []byte("first")})

	//should be in Sub1 not in Sub2
	temp := sub1.Read()
	assert.Equal(t, temp.MessageHeader, headerb)
	assert.Equal(t, temp.MessageBody, []byte("first"))
	log.Printf("First message sent/received")

	time.Sleep(5 * time.Second) //give the os time to bind the tcp port

	pub.Write(common.Message{MessageHeader: headerc, MessageBody: []byte("second")})
	log.Printf("Second message sent/received")

	temp1 := sub.Read()
	temp2 := sub1.Read()

	assert.Equal(t, temp1.MessageHeader, headerb)
	assert.Equal(t, temp1.MessageBody, []byte("second"))

	assert.Equal(t, temp2.MessageHeader, headerb)
	assert.Equal(t, temp2.MessageBody, temp1.MessageBody)
}

func Load(t *testing.T) {
	if testing.Short() || !testing.Verbose() {
		t.SkipNow()
	}
	routingGuide := make(map[string][]string)

	routingGuide["a"] = []string{"b"}
	routingGuide["c"] = []string{"d", "b"}
	routingGuide["d"] = []string{"b"}

	pub, err := publisher.NewPublisher("60010", 1000, 10)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	go pub.Listen()

	var wg1 sync.WaitGroup
	wg1.Add(3)

	r1 := Router{}

	err = r1.Start(routingGuide, wg1, 1000, []string{"localhost:60010"}, 10, time.Second*1, "60011")
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	sub, err := subscriber.NewSubscriber(3000)
	if err != nil {
		t.Error(err)
		t.Fail()
	}

	sub.Subscribe("localhost:60011", []string{"a", "b", "c"})

	headera := [24]byte{}
	copy(headera[:], "a")

	go func() {
		for {
			sub.Read()
		}
	}()

	message := `Chapter 1: A Long-Expected Party

	When Mr. Bilbo Baggins of Bag End announced that he would shortly be celebrating his eleventy-first birthday with a party of special magnificence, there was much talk and excitement in Hobbiton.

	Bilbo was very rich and very peculiar, and had been the wonder of the Shire for sixty years, ever since his remarkable disappearance and unexpected return. The riches he had brought back from his travels had now become a local legend, and it was popularly believed, whatever the old folk might say, that the Hill at Bag End was full of tunnels stuffed with treasure. And if that was not enough for fame, there was also his prolonged vigour to marvel at. Time wore on, but it seemed to have little effect on Mr. Baggins. At ninety he was much the same as at fifty. At ninety-nine they began to call him well-preserved ; but unchanged would have been nearer the mark. There were some that shook their heads and thought this was too much of a good thing; it seemed unfair that anyone should possess (apparently) perpetual youth as well as (reputedly) inexhaustible wealth.

	‘It will have to be paid for,’ they said. ‘It isn’t natural, and trouble will come of it!’

	But so far trouble had not come; and as Mr. Baggins was generous with his money, most people were willing to forgive him his oddities and his good fortune. He remained on visiting terms with his relatives (except, of course, the Sackville-Bagginses), and he had many devoted admirers among the hobbits of poor and unimportant families. But he had no close friends, until some of his younger cousins began to grow up.

	The eldest of these, and Bilbo’s favourite, was young Frodo Baggins. When Bilbo was ninety-nine he adopted Frodo as his heir, and brought him to live at Bag End; and the hopes of the Sackville-Bagginses were finally dashed. Bilbo and Frodo happened to have the same birthday, September 22nd. ‘You had better come and live here, Frodo my lad,’ said Bilbo one day; ‘and then we can celebrate our birthday-parties comfortably together.’ At that time Frodo was still in his tweens, as the hobbits called the irresponsible twenties between childhood and coming of age at thirty-three.

	Continue reading the main story
	Twelve more years passed. Each year the Bagginses had given very lively combined birthday-parties at Bag End; but now it was understood that something quite exceptional was being planned for that autumn. Bilbo was going to be eleventy-one , 111, a rather curious number, and a very respectable age for a hobbit (the Old Took himself had only reached 130); and Frodo was going to be thirty-three , 33, an important number: the date of his ‘coming of age’.

	Tongues began to wag in Hobbiton and Bywater; and rumour of the coming event travelled all over the Shire. The history and character of Mr. Bilbo Baggins became once again the chief topic of conversation; and the older folk suddenly found their reminiscences in welcome demand.

	No one had a more attentive audience than old Ham Gamgee, commonly known as the Gaffer. He held forth at The Ivy Bush , a small inn on the Bywater road; and he spoke with some authority, for he had tended the garden at Bag End for forty years, and had helped old Holman in the same job before that. Now that he was himself growing old and stiff in the joints, the job was mainly carried on by his youngest son, Sam Gamgee. Both father and son were on very friendly terms with Bilbo and Frodo. They lived on the Hill itself, in Number 3 Bagshot Row just below Bag End.

	‘A very nice well-spoken gentlehobbit is Mr. Bilbo, as I’ve always said,’ the Gaffer declared. With perfect truth: for Bilbo was very polite to him, calling him ‘Master Hamfast’, and consulting him constantly upon the growing of vegetables — in the matter of ‘roots’, especially potatoes, the Gaffer was recognized as the leading authority by all in the neighbourhood (including himself).

	‘But what about this Frodo that lives with him?’ asked Old Noakes of Bywater. ‘Baggins is his name, but he’s more than half a Brandybuck, they say. It beats me why any Baggins of Hobbiton should go looking for a wife away there in Buckland, where folks are so queer.’

	‘And no wonder they’re queer,’ put in Daddy Twofoot (the Gaffer’s next-door neighbour), ‘if they live on the wrong side of the Brandywine River, and right agin the Old Forest. That’s a dark bad place, if half the tales be true.’

	‘You’re right, Dad!’ said the Gaffer. ‘Not that the Brandybucks of Buckland live in the Old Forest; but they’re a queer breed, seemingly. They fool about with boats on that big river — and that isn’t natural. Small wonder that trouble came of it, I say. But be that as it may, Mr. Frodo is as nice a young hobbit as you could wish to meet. Very much like Mr. Bilbo, and in more than looks. After all his father was a Baggins. A decent respectable hobbit was Mr. Drogo Baggins; there was never much to tell of him, till he was drownded.’

	‘Drownded?’ said several voices. They had heard this and other darker rumours before, of course; but hobbits have a passion for family history, and they were ready to hear it again.

	‘Well, so they say,’ said the Gaffer. ‘You see: Mr. Drogo, he married poor Miss Primula Brandybuck. She was our Mr. Bilbo’s first cousin on the mother’s side (her mother being the youngest of the Old Took’s daughters); and Mr. Drogo was his second cousin. So Mr. Frodo is his first and second cousin, once removed either way, as the saying is, if you follow me. And Mr. Drogo was staying at Brandy Hall with his father-in-law, old Master Gorbadoc, as he often did after his marriage (him being partial to his vittles, and old Gorbadoc keeping a mighty generous table); and he went out boating on the Brandywine River; and he and his wife were drownded, and poor Mr. Frodo only a child and all.’

	‘I’ve heard they went on the water after dinner in the moonlight,’ said Old Noakes; ‘and it was Drogo’s weight as sunk the boat.’

	‘And I heard she pushed him in, and he pulled her in after him,’ said Sandyman, the Hobbiton miller.

	‘You shouldn’t listen to all you hear, Sandyman,’ said the Gaffer, who did not much like the miller. ‘There isn’t no call to go talking of pushing and pulling. Boats are quite tricky enough for those that sit still without looking further for the cause of trouble. Anyway: there was this Mr. Frodo left an orphan and stranded, as you might say, among those queer Bucklanders, being brought up anyhow in Brandy Hall. A regular warren, by all accounts. Old Master Gorbadoc never had fewer than a couple of hundred relations in the place. Mr. Bilbo never did a kinder deed than when he brought the lad back to live among decent folk.

	‘But I reckon it was a nasty shock for those Sackville-Bagginses. They thought they were going to get Bag End, that time when he went off and was thought to be dead. And then he comes back and orders them off; and he goes on living and living, and never looking a day older, bless him! And suddenly he produces an heir, and has all the papers made out proper. The Sackville-Bagginses won’t never see the inside of Bag End now, or it is to be hoped not.’

	‘There’s a tidy bit of money tucked away up there, I hear tell,’ said a stranger, a visitor on business from Michel Delving in the Westfarthing. ‘All the top of your hill is full of tunnels packed with chests of gold and silver, and jools, by what I’ve heard.’

	‘Then you’ve heard more than I can speak to,’ answered the Gaffer. I know nothing about jools . Mr. Bilbo is free with his money, and there seems no lack of it; but I know of no tunnel-making. I saw Mr. Bilbo when he came back, a matter of sixty years ago, when I was a lad. I’d not long come prentice to old Holman (him being my dad’s cousin), but he had me up at Bag End helping him to keep folks from trampling and trapessing all over the garden while the sale was on. And in the middle of it all Mr. Bilbo comes up the Hill with a pony and some mighty big bags and a couple of chests. I don’t doubt they were mostly full of treasure he had picked up in foreign parts, where there be mountains of gold, they say; but there wasn’t enough to fill tunnels. But my lad Sam will know more about that. He’s in and out of Bag End. Crazy about stories of the old days he is, and he listens to all Mr. Bilbo’s tales. Mr. Bilbo has learned him his letters — meaning no harm, mark you, and I hope no harm will come of it.

	‘Elves and Dragons! I says to him. ‘Cabbages and potatoes are better for me and you. Don’t go getting mixed up in the business of your betters, or you’ll land in trouble too big for you,’ I says to him. And I might say it to others,’ he added with a look at the stranger and the miller.

	But the Gaffer did not convince his audience. The legend of Bilbo’s wealth was now too firmly fixed in the minds of the younger generation of hobbits.

	‘Ah, but he has likely enough been adding to what he brought at first,’ argued the miller, voicing common opinion. ‘He’s often away from home. And look at the outlandish folk that visit him: dwarves coming at night, and that old wandering conjuror, Gandalf, and all. You can say what you like, Gaffer, but Bag End’s a queer place, and its folk are queerer.’

	‘And you can say what you like, about what you know no more of than you do of boating, Mr. Sandyman,’ retorted the Gaffer, disliking the miller even more than usual. ‘If that’s being queer, then we could do with a bit more queerness in these parts. There’s some not far away that wouldn’t offer a pint of beer to a friend, if they lived in a hole with golden walls. But they do things proper at Bag End. Our Sam says that everyone’s going to be invited to the party, and there’s going to be presents, mark you, presents for all — this very month as is.’

	That very month was September, and as fine as you could ask. A day or two later a rumour (probably started by the knowledgeable Sam) was spread about that there were going to be fireworks — fireworks, what is more, such as had not been seen in the Shire for nigh on a century, not indeed since the Old Took died.`

	//End Setup
	for i := 0; i < 5000; i++ {
		for j := 0; j < 100; j++ {
			pub.Write(common.Message{MessageHeader: headera, MessageBody: []byte(message)})
		}
		time.Sleep(10 * time.Millisecond)
	}
}
