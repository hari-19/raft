package raft

//
// Raft tests.
//
// we will use the original test_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "testing"
import "fmt"
import "time"
import "math/rand"
// import "sync/atomic"
import "sync"

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const RaftElectionTimeout = 1000 * time.Millisecond

func TestInitialElectionRA(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (RA): initial election")

	// is a leader elected?
	cfg.checkOneLeader()

	// sleep a bit to avoid racing with followers learning of the
	// election, then check that all peers agree on the term.
	time.Sleep(50 * time.Millisecond)
	term1 := cfg.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %v, but should be at least 1", term1)
	}

	// does the leader+term stay the same if there is no network failure?
	time.Sleep(2 * RaftElectionTimeout)
	term2 := cfg.checkTerms()
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	// there should still be a leader.
	cfg.checkOneLeader()

	cfg.end()
}

func TestReElectionRA(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (RA): election after network failure")

	leader1 := cfg.checkOneLeader()

	// if the leader disconnects, a new one should be elected.
	cfg.disconnect(leader1)
	cfg.checkOneLeader()

	// if the old leader rejoins, that shouldn't
	// disturb the new leader. and the old leader
	// should switch to follower.
	cfg.connect(leader1)
	leader2 := cfg.checkOneLeader()

	// if there's no quorum, no new leader should
	// be elected.
	cfg.disconnect(leader2)
	cfg.disconnect((leader2 + 1) % servers)
	time.Sleep(2 * RaftElectionTimeout)

	// check that the one connected server
	// does not think it is the leader.
	cfg.checkNoLeader()

	// if a quorum arises, it should elect a leader.
	cfg.connect((leader2 + 1) % servers)
	cfg.checkOneLeader()

	// re-join of last node shouldn't prevent leader from existing.
	cfg.connect(leader2)
	cfg.checkOneLeader()

	cfg.end()
}

func TestManyElectionsRA(t *testing.T) {
	servers := 7
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (RA): multiple elections")

	cfg.checkOneLeader()

	iters := 10
	for ii := 1; ii < iters; ii++ {
		// disconnect three nodes
		i1 := rand.Int() % servers
		i2 := rand.Int() % servers
		i3 := rand.Int() % servers
		cfg.disconnect(i1)
		cfg.disconnect(i2)
		cfg.disconnect(i3)

		// either the current leader should still be alive,
		// or the remaining four should elect a new one.
		cfg.checkOneLeader()

		cfg.connect(i1)
		cfg.connect(i2)
		cfg.connect(i3)
	}

	cfg.checkOneLeader()

	cfg.end()
}

func TestBasicAgreeRB(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (RB): basic agreement")

	iters := 3
	for index := 1; index < iters+1; index++ {
		nd, _ := cfg.nCommitted(index)
		if nd > 0 {
			t.Fatalf("some have committed before Start()")
		}

		xindex := cfg.one(index*100, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}

	cfg.end()
}

// check, based on counting bytes of RPCs, that
// each command is sent to each peer just once.
func TestRPCBytesRB(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (RB): RPC byte count")

	cfg.one(99, servers, false)
	bytes0 := cfg.bytesTotal()

	iters := 10
	var sent int64 = 0
	for index := 2; index < iters+2; index++ {
		cmd := randstring(5000)
		xindex := cfg.one(cmd, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
		sent += int64(len(cmd))
	}

	bytes1 := cfg.bytesTotal()
	got := bytes1 - bytes0
	expected := int64(servers) * sent
	if got > expected+50000 {
		t.Fatalf("too many RPC bytes; got %v, expected %v", got, expected)
	}

	cfg.end()
}

// test just failure of followers.
func TestFollowerFailureRB(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (RB): test progressive failure of followers")

	cfg.one(101, servers, false)

	// disconnect one follower from the network.
	leader1 := cfg.checkOneLeader()
	cfg.disconnect((leader1 + 1) % servers)

	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	cfg.one(102, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	cfg.one(103, servers-1, false)

	// disconnect the remaining follower
	leader2 := cfg.checkOneLeader()
	cfg.disconnect((leader2 + 1) % servers)
	cfg.disconnect((leader2 + 2) % servers)

	// submit a command.
	index, _, ok := cfg.rafts[leader2].Start(104)
	if ok != true {
		t.Fatalf("leader rejected Start()")
	}
	if index != 4 {
		t.Fatalf("expected index 4, got %v", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	// check that command 104 did not commit.
	n, _ := cfg.nCommitted(index)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	cfg.end()
}

// test just failure of leaders.
func TestLeaderFailureRB(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (RB): test failure of leaders")

	cfg.one(101, servers, false)

	// disconnect the first leader.
	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)

	// the remaining followers should elect
	// a new leader.
	cfg.one(102, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	cfg.one(103, servers-1, false)

	// disconnect the new leader.
	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)

	// submit a command to each server.
	for i := 0; i < servers; i++ {
		cfg.rafts[i].Start(104)
	}

	time.Sleep(2 * RaftElectionTimeout)

	// check that command 104 did not commit.
	n, _ := cfg.nCommitted(4)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	cfg.end()
}

// test that a follower participates after
// disconnect and re-connect.
func TestFailAgreeRB(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (RB): agreement after follower reconnects")

	cfg.one(101, servers, false)

	// disconnect one follower from the network.
	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % servers)

	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	cfg.one(102, servers-1, false)
	cfg.one(103, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	cfg.one(104, servers-1, false)
	cfg.one(105, servers-1, false)

	// re-connect
	cfg.connect((leader + 1) % servers)

	// the full set of servers should preserve
	// previous agreements, and be able to agree
	// on new commands.
	cfg.one(106, servers, true)
	time.Sleep(RaftElectionTimeout)
	cfg.one(107, servers, true)

	cfg.end()
}

func TestFailNoAgreeRB(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (RB): no agreement if too many followers disconnect")

	cfg.one(10, servers, false)

	// 3 of 5 followers disconnect
	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % servers)
	cfg.disconnect((leader + 2) % servers)
	cfg.disconnect((leader + 3) % servers)

	index, _, ok := cfg.rafts[leader].Start(20)
	if ok != true {
		t.Fatalf("leader rejected Start()")
	}
	if index != 2 {
		t.Fatalf("expected index 2, got %v", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	n, _ := cfg.nCommitted(index)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	// repair
	cfg.connect((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.connect((leader + 3) % servers)

	// the disconnected majority may have chosen a leader from
	// among their own ranks, forgetting index 2.
	leader2 := cfg.checkOneLeader()
	index2, _, ok2 := cfg.rafts[leader2].Start(30)
	if ok2 == false {
		t.Fatalf("leader2 rejected Start()")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %v", index2)
	}

	cfg.one(1000, servers, true)

	cfg.end()
}

func TestConcurrentStartsRB(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (RB): concurrent Start()s")

	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader := cfg.checkOneLeader()
		_, term, ok := cfg.rafts[leader].Start(1)
		if !ok {
			// leader moved on really quickly
			continue
		}

		iters := 5
		var wg sync.WaitGroup
		is := make(chan int, iters)
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				i, term1, ok := cfg.rafts[leader].Start(100 + i)
				if term1 != term {
					return
				}
				if ok != true {
					return
				}
				is <- i
			}(ii)
		}

		wg.Wait()
		close(is)

		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); t != term {
				// term changed -- can't expect low RPC counts
				continue loop
			}
		}

		failed := false
		cmds := []int{}
		for index := range is {
			cmd := cfg.wait(index, servers, term)
			if ix, ok := cmd.(int); ok {
				if ix == -1 {
					// peers have moved on to later terms
					// so we can't expect all Start()s to
					// have succeeded
					failed = true
					break
				}
				cmds = append(cmds, ix)
			} else {
				t.Fatalf("value %v is not an int", cmd)
			}
		}

		if failed {
			// avoid leaking goroutines
			go func() {
				for range is {
				}
			}()
			continue
		}

		for ii := 0; ii < iters; ii++ {
			x := 100 + ii
			ok := false
			for j := 0; j < len(cmds); j++ {
				if cmds[j] == x {
					ok = true
				}
			}
			if ok == false {
				t.Fatalf("cmd %v missing in %v", x, cmds)
			}
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	cfg.end()
}

func TestRejoinRB(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (RB): rejoin of partitioned leader")

	cfg.one(101, servers, true)

	// leader network failure
	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)

	// make old leader try to agree on some entries
	cfg.rafts[leader1].Start(102)
	cfg.rafts[leader1].Start(103)
	cfg.rafts[leader1].Start(104)

	// new leader commits, also for index=2
	cfg.one(103, 2, true)

	// new leader network failure
	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)

	// old leader connected again
	cfg.connect(leader1)

	cfg.one(104, 2, true)

	// all together now
	cfg.connect(leader2)

	cfg.one(105, servers, true)

	cfg.end()
}

func TestBackupRB(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (RB): leader backs up quickly over incorrect follower logs")

	cfg.one(rand.Int(), servers, true)

	// put leader and one follower in a partition
	leader1 := cfg.checkOneLeader()
	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)

	// submit lots of commands that won't commit
	for i := 0; i < 50; i++ {
		cfg.rafts[leader1].Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	cfg.disconnect((leader1 + 0) % servers)
	cfg.disconnect((leader1 + 1) % servers)

	// allow other partition to recover
	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)

	// lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}

	// now another partitioned leader and one follower
	leader2 := cfg.checkOneLeader()
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	cfg.disconnect(other)

	// lots more commands that won't commit
	for i := 0; i < 50; i++ {
		cfg.rafts[leader2].Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	// bring original leader back to life,
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}
	cfg.connect((leader1 + 0) % servers)
	cfg.connect((leader1 + 1) % servers)
	cfg.connect(other)

	// lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}

	// now everyone
	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}
	cfg.one(rand.Int(), servers, true)

	cfg.end()
}

func TestCountRB(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (RB): RPC counts aren't too high")

	rpcs := func() (n int) {
		for j := 0; j < servers; j++ {
			n += cfg.rpcCount(j)
		}
		return
	}

	leader := cfg.checkOneLeader()

	total1 := rpcs()

	if total1 > 30 || total1 < 1 {
		t.Fatalf("too many or few RPCs (%v) to elect initial leader\n", total1)
	}

	var total2 int
	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader = cfg.checkOneLeader()
		total1 = rpcs()

		iters := 10
		starti, term, ok := cfg.rafts[leader].Start(1)
		if !ok {
			// leader moved on really quickly
			continue
		}
		cmds := []int{}
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31())
			cmds = append(cmds, x)
			index1, term1, ok := cfg.rafts[leader].Start(x)
			if term1 != term {
				// Term changed while starting
				continue loop
			}
			if !ok {
				// No longer the leader, so term has changed
				continue loop
			}
			if starti+i != index1 {
				t.Fatalf("Start() failed")
			}
		}

		for i := 1; i < iters+1; i++ {
			cmd := cfg.wait(starti+i, servers, term)
			if ix, ok := cmd.(int); ok == false || ix != cmds[i-1] {
				if ix == -1 {
					// term changed -- try again
					continue loop
				}
				t.Fatalf("wrong value %v committed for index %v; expected %v\n", cmd, starti+i, cmds)
			}
		}

		failed := false
		total2 = 0
		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); t != term {
				// term changed -- can't expect low RPC counts
				// need to keep going to update total2
				failed = true
			}
			total2 += cfg.rpcCount(j)
		}

		if failed {
			continue loop
		}

		if total2-total1 > (iters+1+3)*3 {
			t.Fatalf("too many RPCs (%v) for %v entries\n", total2-total1, iters)
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	time.Sleep(RaftElectionTimeout)

	total3 := 0
	for j := 0; j < servers; j++ {
		total3 += cfg.rpcCount(j)
	}

	if total3-total2 > 3*20 {
		t.Fatalf("too many RPCs (%v) for 1 second of idleness\n", total3-total2)
	}

	cfg.end()
}


const MAXLOGSIZE = 2000

func snapcommon(t *testing.T, name string, disconnect bool, reliable bool, crash bool) {
	iters := 30
	servers := 3
	cfg := make_config(t, servers, !reliable, true)
	defer cfg.cleanup()

	cfg.begin(name)

	cfg.one(rand.Int(), servers, true)
	leader1 := cfg.checkOneLeader()

	for i := 0; i < iters; i++ {
		victim := (leader1 + 1) % servers
		sender := leader1
		if i%3 == 1 {
			sender = (leader1 + 1) % servers
			victim = leader1
		}

		if disconnect {
			cfg.disconnect(victim)
			cfg.one(rand.Int(), servers-1, true)
		}
		if crash {
			cfg.crash1(victim)
			cfg.one(rand.Int(), servers-1, true)
		}

		// perhaps send enough to get a snapshot
		nn := (SnapShotInterval / 2) + (rand.Int() % SnapShotInterval)
		for i := 0; i < nn; i++ {
			cfg.rafts[sender].Start(rand.Int())
		}

		// let applier threads catch up with the Start()'s
		if disconnect == false && crash == false {
			// make sure all followers have caught up, so that
			// an InstallSnapshot RPC isn't required for
			cfg.one(rand.Int(), servers, true)
		} else {
			cfg.one(rand.Int(), servers-1, true)
		}

		if cfg.LogSize() >= MAXLOGSIZE {
			cfg.t.Fatalf("Log size too large")
		}
		if disconnect {
			// reconnect a follower, who maybe behind and
			// needs to rceive a snapshot to catch up.
			cfg.connect(victim)
			cfg.one(rand.Int(), servers, true)
			leader1 = cfg.checkOneLeader()
		}
		if crash {
			cfg.start1(victim, cfg.applierSnap)
			cfg.connect(victim)
			cfg.one(rand.Int(), servers, true)
			leader1 = cfg.checkOneLeader()
		}
	}
	cfg.end()
}

