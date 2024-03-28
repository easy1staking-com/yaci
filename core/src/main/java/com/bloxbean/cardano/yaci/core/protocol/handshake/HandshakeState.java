package com.bloxbean.cardano.yaci.core.protocol.handshake;

import com.bloxbean.cardano.yaci.core.config.YaciConfig;
import com.bloxbean.cardano.yaci.core.protocol.Message;

public enum HandshakeState implements HandshakeStateBase {
    Propose {
        @Override
        public HandshakeState nextState(Message message) {
            return Confirm;
        }

        @Override
        public boolean hasAgency() {
            return !YaciConfig.INSTANCE.isServer();
        }
    },
    Confirm {
        @Override
        public HandshakeState nextState(Message message) {
            return Done;
        }

        @Override
        public boolean hasAgency() {
            return YaciConfig.INSTANCE.isServer();
        }
    },
    Done {
        @Override
        public HandshakeState nextState(Message message) {
            return this;
        }

        @Override
        public boolean hasAgency() {
            return false;
        }
    }

}

//public interface State {
//    public abstract State nextState();
//    public abstract boolean hasAgency();
//}
//
////class Propose1 implements State {
////        private Message message;
////        public Propose(ProposedVersions proposedVersions) {
////            this.message = proposedVersions;
////        }
////
////        @Override
////        public State nextState() {
////            return new Confirm(null);
////        }
////
////        @Override
////        public boolean hasAgency() {
////            return true;
////        }
////}
////
////class Confirm1 implements State {
////        private Message message;
////        public Confirm(Message message) {
////            this.message = message;
////        }
////
////        @Override
////        public State nextState() {
////            return new Done();
////        }
////
////        @Override
////        public boolean hasAgency() {
////            return false;
////        }
////}
////
//// class Done1 implements State {
////        @Override
////        public State nextState() {
////            return this;
////        }
////
////        @Override
////        public boolean hasAgency() {
////            return false;
////        }
////    };
//
//