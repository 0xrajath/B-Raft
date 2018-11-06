package main

import (
	"fmt"
	"log"
	rand "math/rand"
	"net"
	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/lab-2-raft-rajathalex/pb"
)

// Messages that can be passed from the Raft RPC server to the main loop for AppendEntries
type AppendEntriesInput struct {
	arg      *pb.AppendEntriesArgs
	response chan pb.AppendEntriesRet
}

// Messages that can be passed from the Raft RPC server to the main loop for VoteInput
type VoteInput struct {
	arg      *pb.RequestVoteArgs
	response chan pb.RequestVoteRet
}

// Struct off of which we shall hang the Raft service
type Raft struct {
	AppendChan chan AppendEntriesInput
	VoteChan   chan VoteInput
}

func (r *Raft) AppendEntries(ctx context.Context, arg *pb.AppendEntriesArgs) (*pb.AppendEntriesRet, error) {
	c := make(chan pb.AppendEntriesRet)
	r.AppendChan <- AppendEntriesInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

func (r *Raft) RequestVote(ctx context.Context, arg *pb.RequestVoteArgs) (*pb.RequestVoteRet, error) {
	c := make(chan pb.RequestVoteRet)
	r.VoteChan <- VoteInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

// Compute a random duration in milliseconds
func randomDuration(r *rand.Rand) time.Duration {
	// Constant
	const DurationMax = 20000
	const DurationMin = 5000
	return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
}

// Restart the supplied timer using a random timeout based on function above
func restartTimer(timer *time.Timer, r *rand.Rand) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(randomDuration(r))
}

// Stop the supplied timer - For use when Candidate becomes Leader after majority votes
func stopTimer(timer *time.Timer) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
}

// Restart the heartbeat timer
func restartHBTimer(timer *time.Timer) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(1000 * time.Millisecond)
}

// Stop the heartbeat timer - For use when Leader goes back to being Follower
func stopHBTimer(timer *time.Timer) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
}

// Launch a GRPC service for this Raft peer.
func RunRaftServer(r *Raft, port int) {
	// Convert port to a string form
	portString := fmt.Sprintf(":%d", port)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	pb.RegisterRaftServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func connectToPeer(peer string) (pb.RaftClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewRaftClient(nil), err
	}
	return pb.NewRaftClient(conn), nil
}

// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int, totNumNodes int) {
	raft := Raft{AppendChan: make(chan AppendEntriesInput), VoteChan: make(chan VoteInput)}
	// Start in a Go routine so it doesn't affect us.
	go RunRaftServer(&raft, port)

	peerClients := make(map[string]pb.RaftClient)

	for _, peer := range *peers {
		client, err := connectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}

		peerClients[peer] = client
		log.Printf("Connected to %v", peer)
	}

	type AppendResponse struct {
		ret  *pb.AppendEntriesRet
		err  error
		peer string
	}

	type VoteResponse struct {
		ret  *pb.RequestVoteRet
		err  error
		peer string
	}
	appendResponseChan := make(chan AppendResponse)
	voteResponseChan := make(chan VoteResponse)

	// Create a timer and start running it
	timer := time.NewTimer(randomDuration(r))
	// Creating heartbeatTimer - This ensures it's not called before the first election timeout
	heartbeatTimer := time.NewTimer(100000 * time.Millisecond)

	//log.Printf("Total number of nodes : %v", totNumNodes)
	// State -- To add more terms
	var currentTerm int64 //Default is 0
	var votes int
	var votedFor string
	var currentLeader string

	// Run forever handling inputs from various channels
	for {
		select {
		case <-timer.C:
			// The Election timer went off - Convert to candidate
			log.Printf("Election Timeout!! - Convert to Candidate")
			votes = 0 //Resets vote count

			//Election
			currentTerm++
			log.Printf("Current term increased to %v due to election timeout", currentTerm)
			votes++       //Votes for itself
			votedFor = id //Since new term has started and it has voted for itself

			for p, c := range peerClients {
				// Send in parallel so we don't wait for each client.
				go func(c pb.RaftClient, p string) {
					ret, err := c.RequestVote(context.Background(), &pb.RequestVoteArgs{Term: currentTerm, CandidateID: id})
					voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p}
				}(c, p)
			}
			// This will also take care of any pesky timeouts that happened while processing the operation.
			restartTimer(timer, r)
		case <-heartbeatTimer.C:
			if id == currentLeader { //Only run this if it is current leader
				//Heartbeats
				log.Printf("Sending heartbeats from leader:%v,%v in term:%v", id, currentLeader, currentTerm)

				for p, c := range peerClients {
					// Send in parallel so we don't wait for each client.
					go func(c pb.RaftClient, p string) {
						ret, err := c.AppendEntries(context.Background(), &pb.AppendEntriesArgs{Term: currentTerm, LeaderID: id})
						appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p}
					}(c, p)
				}

				// This will also take care of any pesky timeouts that happened while processing the operation.
				restartHBTimer(heartbeatTimer)
			}
		case op := <-s.C:
			// We received an operation from a client
			// TODO: Figure out if you can actually handle the request here. If not use the Redirect result to send the
			// client elsewhere.
			// TODO: Use Raft to make sure it is safe to actually run the command -- i.e Do HandleCommand only after it's been committed
			s.HandleCommand(op)
		case ae := <-raft.AppendChan:
			// We received an AppendEntries request from a Raft peer
			// TODO figure out what to do here, what we do is entirely wrong.
			log.Printf("Received append entry from %v", ae.arg.LeaderID)

			//Might need to fix this logic later
			if currentTerm < ae.arg.Term {
				log.Printf("Term incremented. My term: %v. Appender term: %v", currentTerm, ae.arg.Term)
				currentTerm = ae.arg.Term
				votedFor = ""            //Resetting votedFor as I've not yet voted for anyone in this updated term
				votes = 0                //Resetting my vote count
				if currentLeader == id { //If I am the leader
					stopHBTimer(heartbeatTimer) //Since Leader stepping down to follower
				}
				currentLeader = ae.arg.LeaderID // Assigning new leader with the higher term
				restartTimer(timer, r)
			} else if currentTerm == ae.arg.Term {
				log.Printf("Leader is %v for term %v", ae.arg.LeaderID, currentTerm)
				currentLeader = ae.arg.LeaderID //Assigning leader for whom we voted earlier
				//votes = 0                       //Required??
				restartTimer(timer, r)
			} else { //Receiving Stale Term
				log.Printf("Append request from %v rejected as appender term < my term. My term: %v. Appender term: %v", ae.arg.LeaderID, currentTerm, ae.arg.Term)
				ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: false}
			}

			//ae.response <- pb.AppendEntriesRet{Term: 1, Success: true}
			// This will also take care of any pesky timeouts that happened while processing the operation.

		case vr := <-raft.VoteChan:
			// We received a RequestVote RPC from a raft peer
			// TODO: Fix this.
			log.Printf("Received vote request from %v", vr.arg.CandidateID)

			if currentTerm <= vr.arg.Term {
				if currentTerm < vr.arg.Term { //Current term is less than Requester term
					currentTerm = vr.arg.Term
					votedFor = vr.arg.CandidateID
					votes = 0                //Reset my own votes incase I was a candidate
					if currentLeader == id { //If I am the leader
						stopHBTimer(heartbeatTimer) //Since Leader stepping down to follower
					}
					log.Printf("Voted for %v due to term increase", vr.arg.CandidateID)
					vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: true} //Voted for Requester
					restartTimer(timer, r)

				} else { //Current term is equal to Requester term
					if votedFor == "" { //Then you can vote as you've not voted yet
						votedFor = vr.arg.CandidateID
						log.Printf("Voted for %v as I've not yet voted this term", vr.arg.CandidateID)
						vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: true} //Voted for Requester
						restartTimer(timer, r)
					} else { //Reject vote as you've already voted this term
						log.Printf("Vote request from %v rejected as I've already voted in this term: %v for %v", vr.arg.CandidateID, currentTerm, votedFor)
						vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: false}
					}
				}
			} else { //Reject vote request
				log.Printf("Vote request from %v rejected as requester term < my term. My term: %v. Requester term: %v", vr.arg.CandidateID, currentTerm, vr.arg.Term)
				vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: false}
			}

			//vr.response <- pb.RequestVoteRet{Term: 1, VoteGranted: false}

		case vr := <-voteResponseChan:
			// We received a response to a previou vote request.
			// TODO: Fix this
			if vr.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				log.Printf("Error calling RPC %v", vr.err)
			} else {
				//To check Term of response received

				log.Printf("Got response to vote request from %v", vr.peer)
				log.Printf("Peer %s granted %v term %v", vr.peer, vr.ret.VoteGranted, vr.ret.Term)

				if currentTerm < vr.ret.Term { //Some other node is at a higher term - Candidate changes to Follower
					log.Printf("Term incremented. My term: %v. Voter term: %v", currentTerm, vr.ret.Term)
					log.Printf("Stepping down to Follower")
					currentTerm = vr.ret.Term
					votedFor = "" //Resetting votedFor as I've not yet voted for anyone in this updated term
					votes = 0     //Resetting my vote count
					restartTimer(timer, r)
				} else if currentTerm == vr.ret.Term {
					//Vote Granted by peer
					if vr.ret.VoteGranted {
						votes++
						log.Printf("Peer %v voted %v in term %v. Vote Count: %v ", vr.peer, vr.ret.VoteGranted, currentTerm, votes)
					}

					if votes > (totNumNodes/2) && currentLeader != id { //Majority vote achieved - Candidate changes to Leader
						log.Printf("Got Majority vote count of %v among %v nodes", votes, totNumNodes)
						log.Printf("Converting to Leader from Candidate")
						currentLeader = id                                      //Assigning self as Leader
						stopTimer(timer)                                        //Stopping Election timer since it has become leader
						heartbeatTimer = time.NewTimer(1000 * time.Millisecond) //Starting heartbeatTimer
					}
				} else { //Receiving Vote response for Stale Term
					log.Printf("Do Nothing. Received vote response for stale term. My term: %v. Voter term: %v", currentTerm, vr.ret.Term)
				}

			}
		case ar := <-appendResponseChan:
			// We received a response to a previous AppendEntries RPC call
			if ar.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				log.Printf("Error calling RPC %v", ar.err)
			} else {
				log.Printf("Got append entries response from %v", ar.peer)
				log.Printf("Peer %s granted Success: %v in term %v", ar.peer, ar.ret.Success, ar.ret.Term)

				if currentTerm < ar.ret.Term { //Some other node is at a higher term - Leader changes to Follower
					log.Printf("Term incremented. My term: %v. Appender term: %v", currentTerm, ar.ret.Term)
					log.Printf("Stepping down to Follower")
					currentTerm = ar.ret.Term
					votedFor = "" //Resetting votedFor as I've not yet voted for anyone in this updated term
					votes = 0     //Resetting my vote count
					restartTimer(timer, r)
					stopHBTimer(heartbeatTimer) //Since Leader stepping down to follower
				} else if currentTerm == ar.ret.Term {
					//Do something here - I'm guessing log replication happens here.
					log.Printf("Some log replication I guess") //To change
				} else { //Receiving Append response for Stale Term
					log.Printf("Do Nothing. Received append response for stale term. My term: %v. Appender term: %v", currentTerm, ar.ret.Term)
				}

			}

		}
	}
	log.Printf("Strange to arrive here")
}
