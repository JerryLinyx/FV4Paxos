package paxos

import (
	"coms4113/hw5/pkg/base"
	"fmt"
)

// Fill in the function to lead the program to a state where A2 rejects the Accept Request of P1
func ToA2RejectP1() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose && s1.proposer.N == 1 && s1.proposer.SessionId == 1
	}

	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept && s1.proposer.N == 1 && s1.proposer.ResponseCount == 0 &&
			s1.proposer.SuccessCount == 0 && s1.proposer.SessionId == 1
	}

	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose && s3.proposer.N >= 2 &&
			s.Nodes()["s2"].(*Server).n_p >= 2
	}

	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept
	}

	p1RejectedByp2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		valid := false
		rejectionFromp2 := false
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && m.To() == s1.Address() && m.From() == s2.Address() {
					rejectionFromp2 = true
				}
			}
			if rejectionFromp2 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1's accept request got rejected by p2")
		}
		return valid
	}

	return []func(s *base.State) bool{p1PreparePhase, p1AcceptPhase, p3PreparePhase,
		p3AcceptPhase, p1RejectedByp2}
}

// Fill in the function to lead the program to a state where a consensus is reached in Server 3.
func ToConsensusCase5() []func(s *base.State) bool {
	s3DecidePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Decide && s3.proposer.V == "v3"
	}

	s3KnowConsensus := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.agreedValue == "v3"
	}

	return []func(s *base.State) bool{s3DecidePhase, s3KnowConsensus}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected
func NotTerminate1() []func(s *base.State) bool {
	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Accept && s1.proposer.N == 1 &&
			s1.proposer.SuccessCount == 0 && s1.proposer.ResponseCount == 0
		if valid {
			fmt.Println("... p1 ready to send Accept with n=1")
		}
		return valid
	}

	higherPrepare := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := s1.n_p >= 2 && s2.n_p >= 2 && s3.n_p >= 2 &&
			s1.proposer.ResponseCount == 0 && s1.proposer.SuccessCount == 0 &&
			s1.proposer.SessionId == 1 && s1.proposer.N == 1 && s1.proposer.Phase == Accept
		if valid {
			fmt.Println("... all acceptors raised n_p >=2")
		}
		return valid
	}

	p1RejectResponsesPending := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase != Accept || s1.proposer.N != 1 {
			return false
		}
		if s1.proposer.ResponseCount != 0 || s1.proposer.SuccessCount != 0 {
			return false
		}
		seen := map[base.Address]bool{}
		for _, m := range s.Network {
			if resp, ok := m.(*AcceptResponse); ok && resp.To() == s1.Address() && !resp.Ok && resp.SessionId == s1.proposer.SessionId {
				seen[resp.From()] = true
			}
		}
		valid := len(seen) == 3
		if valid {
			fmt.Println("... p1 reject responses are pending")
		}
		return valid
	}

	return []func(s *base.State) bool{p1AcceptPhase, higherPrepare, p1RejectResponsesPending}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P3 are rejected
func NotTerminate2() []func(s *base.State) bool {
	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept && s3.proposer.N >= 2 &&
			s3.proposer.SuccessCount == 0 && s3.proposer.ResponseCount == 0
		if valid {
			fmt.Println("... p3 ready to send Accept with n", s3.proposer.N)
		}
		return valid
	}

	higherPrepare := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		target := s3.proposer.N + 1
		valid := s1.n_p >= target && s2.n_p >= target && s3.n_p >= target &&
			s3.proposer.ResponseCount == 0 && s3.proposer.SuccessCount == 0 &&
			s3.proposer.Phase == Accept
		if valid {
			fmt.Println("... all acceptors raised n_p >=", target)
		}
		return valid
	}

	p3RejectResponsesPending := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		if s3.proposer.Phase != Accept {
			return false
		}
		if s3.proposer.ResponseCount != 0 || s3.proposer.SuccessCount != 0 {
			return false
		}
		seen := map[base.Address]bool{}
		for _, m := range s.Network {
			if resp, ok := m.(*AcceptResponse); ok && resp.To() == s3.Address() && !resp.Ok && resp.SessionId == s3.proposer.SessionId {
				seen[resp.From()] = true
			}
		}
		valid := len(seen) == 3
		if valid {
			fmt.Println("... p3 reject responses are pending")
		}
		return valid
	}

	return []func(s *base.State) bool{p3AcceptPhase, higherPrepare, p3RejectResponsesPending}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected again.
func NotTerminate3() []func(s *base.State) bool {
	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Accept && s1.proposer.SuccessCount == 0 && s1.proposer.ResponseCount == 0
		if valid {
			fmt.Println("... p1 ready for another Accept round n", s1.proposer.N)
		}
		return valid
	}

	higherPrepare := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		target := s1.proposer.N + 1
		valid := s1.n_p >= target && s2.n_p >= target && s3.n_p >= target &&
			s1.proposer.ResponseCount == 0 && s1.proposer.SuccessCount == 0 &&
			s1.proposer.Phase == Accept
		if valid {
			fmt.Println("... acceptors raised n_p >=", target, "before p1 Accept")
		}
		return valid
	}

	p1RejectResponsesPending := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase != Accept {
			return false
		}
		if s1.proposer.ResponseCount != 0 || s1.proposer.SuccessCount != 0 {
			return false
		}
		seen := map[base.Address]bool{}
		for _, m := range s.Network {
			if resp, ok := m.(*AcceptResponse); ok && resp.To() == s1.Address() && !resp.Ok && resp.SessionId == s1.proposer.SessionId {
				seen[resp.From()] = true
			}
		}
		valid := len(seen) == 3
		if valid {
			fmt.Println("... p1 reject responses are pending again")
		}
		return valid
	}

	return []func(s *base.State) bool{p1AcceptPhase, higherPrepare, p1RejectResponsesPending}
}

// Fill in the function to lead the program to make P1 propose first, then P3 proposes, but P1 get rejects in
// Accept phase
func concurrentProposer1() []func(s *base.State) bool {
	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept && s1.proposer.N == 1 &&
			s1.proposer.ResponseCount == 0 && s1.proposer.SuccessCount == 0
	}

	higherPrepare := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		return s1.proposer.Phase == Accept && s1.proposer.N == 1 &&
			s2.n_p >= 2 && s3.n_p >= 2
	}

	rejectsPending := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase != Accept || s1.proposer.N != 1 {
			return false
		}
		seen := map[base.Address]bool{}
		for _, m := range s.Network {
			if resp, ok := m.(*AcceptResponse); ok && resp.To() == s1.Address() && !resp.Ok && resp.SessionId == s1.proposer.SessionId {
				seen[resp.From()] = true
			}
		}
		return len(seen) == 3
	}

	return []func(s *base.State) bool{p1AcceptPhase, higherPrepare, rejectsPending}
}

// Fill in the function to lead the program continue  P3's proposal  and reaches consensus at the value of "v3".
func concurrentProposer2() []func(s *base.State) bool {
	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept && s3.proposer.V == "v3"
	}

	p3DecidePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return (s3.proposer.Phase == Decide && s3.proposer.V == "v3") || s3.agreedValue == "v3"
	}

	return []func(s *base.State) bool{p3AcceptPhase, p3DecidePhase}
}
