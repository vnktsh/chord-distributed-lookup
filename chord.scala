import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import java.io.File
import java.security.MessageDigest
import akka.actor._
import akka.actor.ActorRef
import scala.concurrent.ExecutionContext
import akka.actor.Actor
import akka.actor.ActorDSL._
import akka.actor.ActorSystem
import akka.actor.Props
import scala.collection.mutable._
import scala.util.control.Breaks._
import akka.actor.ActorSelection
import Math._
import java.nio._
import java.util.Arrays
import java.math.BigInteger

object Globs {
    val m=4
    var bootNode:ActorRef = null
    var bootNode_suc:ActorRef = null
    var prints:Integer = 0
    val limits:Integer = 1
}

case object Print_Node

case object Join_Chord
case class Continue_Booting(id_predecessor:ActorRef,id_successor:ActorRef)
case class Building_FTable(id_successor:ActorRef,i:Integer)
case object Update_Others
case object Iterate_FTable
case class Continue_Updating(original_sender:ActorRef,id_predecessor:ActorRef,ftable_index:Integer)
case class Update_FTable(original_sender:ActorRef,s:ActorRef,i:Integer)
case class Set_Predecessor(p: ActorRef)
case class Get_Successor(id:Integer,purpose:String,original_sender:ActorRef,reply_to:ActorRef)
case class Finding_Predecessor(id:Integer,purpose:String,original_sender:ActorRef,
                                    nearest_node:ActorRef,nearest_node_suc:ActorRef)

class Finger_Tuple (var start:Integer,var node:ActorRef)

class Node() extends Actor {
    import Globs._
    var node_id:Integer= null
    var b_prog:Integer = 0
    var Predecessor:ActorRef = null
    var ftb= new ArrayBuffer[Finger_Tuple]
   
    override def preStart():Unit = {
        //println("path: "+self.path.toString())
        node_id = consistent_hash(self.path.toString())
        println("\n------Finger table for nodeid: "+node_id+"------")
        println("--------------------------------------")
        for (i <- 1 to m) {
            var si:Int = ((this.node_id + pow(2,i-1)) % pow(2,m)).toInt
            var ft = new Finger_Tuple(si,self)
            ftb += ft
            //println("node"+this.ftb(i).node)
            println((i-1)+". start: "+ftb(i-1).start)
        }
        this.Predecessor = self
        println("--------------------------------------\n")
    }

    def getSuccessor():ActorRef = {
        return this.ftb(0).node
    }

    def getNodeID(n:ActorRef):Integer = {
        return consistent_hash(n.path.toString())
    }

    def closest_preceding_finger(id:Integer,purpose:String,original_sender:ActorRef,
                                    nearest_node:ActorRef,nearest_node_suc:ActorRef):Unit = {

        for ( i <- m-1 to 0 by -1) {

            if(prints < limits ) {
                //println("closest_out_for id: "+id+" p: "+purpose+" os: "+getNodeID(original_sender)+
                  //              " nn: "+getNodeID(nearest_node)+" nns-s: "+getNodeID(nearest_node_suc))
                println("closest-fi: "+getNodeID(this.ftb(i).node)+" in ("+this.node_id+","+id+")")
                    prints = prints + 1
            }

            if(lies_in(getNodeID(this.ftb(i).node),this.node_id,id,0,0)) {
                if(prints < limits ) {
                    println("closest_for id: "+id+" p: "+purpose+" os: "+getNodeID(original_sender)+
                                    " nn: "+getNodeID(nearest_node)+" nns-s: "+getNodeID(nearest_node_suc)) 
                                    prints = prints + 1           
                }
                this.ftb(i).node ! Get_Successor(id,purpose,original_sender,self)
                return
            }
        }
           if(prints < limits ) {
                println("closest_o id: "+id+" p: "+purpose+" os: "+getNodeID(original_sender)+
                                " nn: "+getNodeID(nearest_node)+" nns-s: "+getNodeID(nearest_node_suc)) 
                                prints = prints + 1           
            }
        self ! Finding_Predecessor(id,purpose,original_sender,self,this.getSuccessor())
        return
    }

    override def receive: Receive = {

        case Finding_Predecessor(id:Integer,purpose:String,original_sender:ActorRef,
                                    nearest_node:ActorRef,nearest_node_suc:ActorRef) => {
            if(prints < limits) {

                println("FP:? "+getNodeID(self)+":: id: "+id+" p: "+purpose+" os: "+getNodeID(original_sender)+
                                " nn: "+getNodeID(nearest_node)+" nns-s: "+getNodeID(nearest_node_suc)) 
                                prints = prints + 1           
            }

            if(prints < limits ) {
                println("FP:? "+getNodeID(self)+":: is lies in? "+id+" in ("+getNodeID(nearest_node)+","+getNodeID(nearest_node_suc)+"]")
                    prints = prints + 1
            }
            
            if(lies_in(id,getNodeID(nearest_node),getNodeID(nearest_node_suc),0,1)) {

                purpose.split(":")(0) match {
                    case "booting" => {
                        println("FP:? "+getNodeID(self)+":: case booting")
                        original_sender ! Continue_Booting(nearest_node,nearest_node_suc)
                    }
                    case "building" => {
                        println("FP:? "+getNodeID(self)+":: case building")
                        original_sender ! Building_FTable(nearest_node_suc,purpose.split(":")(1).toInt)
                    }
                    case "updating" => {
                        println("FP:? "+getNodeID(self)+":: case updating")
                        original_sender ! Continue_Updating(original_sender,nearest_node,purpose.split(":")(1).toInt)
                    }
                }
                
            } else {
                closest_preceding_finger(id,purpose,original_sender,nearest_node,nearest_node_suc)
            }   
        }

        case Get_Successor(id:Integer,purpose:String,original_sender:ActorRef,reply_to:ActorRef) => {
           if(prints < limits ) {
                println("Get_Successor?: "+getNodeID(self)+" id: "+id+" p: "+purpose+" os: "+getNodeID(original_sender)+ 
                                    " reply_to: "+getNodeID(reply_to))
                                prints = prints + 1           
            }
            reply_to ! Finding_Predecessor(id,purpose,original_sender,self,this.getSuccessor())

        }

        case Set_Predecessor(p: ActorRef) => {
            println("Set_Predecessor?: p "+getNodeID(p)+" this.id: "+this.node_id)
            this.Predecessor = p
        }

        case Print_Node => {
            val fullPrint:Boolean = true 
            if(fullPrint) {
                println("\nFor node: "+this.node_id+"=> pred:"+getNodeID(this.Predecessor)+" ; succ:"+getNodeID(getSuccessor()))
                println("\n------Finger table for nodeid: "+node_id+"------")
                println("--------------------------------------")
                for (i <- 1 to m) {
                    println((i-1)+". start - node: "+ftb(i-1).start+" - "+getNodeID(ftb(i-1).node))
                }
                println("--------------------------------------\n")            
            } else {
                println("\nFor node: "+this.node_id+"=> pred:"+getNodeID(this.Predecessor)+" ; succ:"+getNodeID(getSuccessor()))
            }

        }

        case Update_FTable(original_sender:ActorRef,s:ActorRef,i:Integer) => {
            println("Update_FTable?: "+getNodeID(self)+":: os: "+getNodeID(original_sender)+" s "+getNodeID(s)+" i: "+i)
            println("Update_FTable?: "+getNodeID(self)+":: i: "+i+" lies in? "+getNodeID(s)+" in ["+this.node_id+","+getNodeID(this.ftb(i).node)+")")
            if (lies_in(getNodeID(s),this.node_id,getNodeID(ftb(i).node),1,0)) {
                ftb(i).node = s
                if((getNodeID(self) == getNodeID(bootNode)) && (i == 0)) {
                    println("Updating bootnode suc")
                    bootNode_suc = ftb(i).node
                }
                
                println("Update_FTable?: "+getNodeID(self)+":: i: "+i+" trying to call this.pred for UFT pred: "+ getNodeID(this.Predecessor))
                if(getNodeID(original_sender) != getNodeID(this.Predecessor)) {
                    println("Update_FTable?: "+getNodeID(self)+":: i: "+i+" called pred")
                    this.Predecessor ! Update_FTable(original_sender,s,i)
                }
            }
        }
 
        case Continue_Updating(original_sender:ActorRef,id_predecessor:ActorRef,ftable_index:Integer) => {
            println("Continue_Updating?: "+getNodeID(self)+" :: os: "+getNodeID(original_sender)+" pred "+getNodeID(id_predecessor)+" f_i: "+ftable_index)
            
            if(getNodeID(original_sender) != getNodeID(id_predecessor)) {
                id_predecessor ! Update_FTable(original_sender,self,ftable_index)
            }
        }

        case Update_Others => {
            println("\nUpdate_Others?: "+getNodeID(self)+" ::")
            for (i <- 0 to m-1) {
                var last_i:Integer = this.node_id - (pow(2,i)).toInt
                last_i = ((( last_i % pow(2,m)) + pow(2,m)) % pow(2,m)).toInt
                
                if (last_i >= 0) {
                    //println("\nUpdate_Others?: "+getNodeID(self)+" ::")
                    println("\n\nUpdate_Others?:"+getNodeID(self)+" :: Finidng pred for last_i "+last_i)
                    self ! Finding_Predecessor(last_i,"updating:"+i,self,self,this.getSuccessor())
                }
            }
        }

        case Building_FTable(id_successor:ActorRef,i:Integer) => {
            println("Building_FTable?: "+getNodeID(self)+ ":: id_suc: "+getNodeID(id_successor)+" i: "+i)
            this.ftb(i).node = id_successor
            self ! Iterate_FTable
        }

        case Iterate_FTable => {
            //for(i <- 0 to m-2) {
            if(b_prog >= m-1){
                self ! Update_Others
            } else {
                var i:Integer = b_prog
                println("Iterate_FTable:? "+getNodeID(self)+":: bprog: "+b_prog+" lies in? "+this.ftb(i+1).start+" in ["+this.node_id+","+getNodeID(this.ftb(i).node)+")")
                if(lies_in(this.ftb(i+1).start,this.node_id,getNodeID(this.ftb(i).node),1,0)) {
                    println("Iterate_FTable:? "+getNodeID(self)+" ::"+i+" updating "+(i+1)+" table entry")
                    this.ftb(i+1).node = this.ftb(i).node
                    b_prog = b_prog + 1

                    self ! Iterate_FTable
                    
                } else {
                    b_prog = b_prog + 1
                    println("Iterate_FTable:? "+getNodeID(self)+":: b_prog "+b_prog+" Find pred for "+node_id+" using "+getNodeID(bootNode))
                    bootNode ! Finding_Predecessor(ftb(i+1).start,"building:"+(i+1),self,bootNode,bootNode_suc)
                }
            }
            //}
        }

        case Continue_Booting(id_predecessor:ActorRef,id_successor:ActorRef) => {
            println("\nContinue_Booting:? "+getNodeID(self)+":: Got parmas as pred,suc: "+getNodeID(id_predecessor)+","+getNodeID(id_successor))
            this.ftb(0).node = id_successor
            this.Predecessor = id_predecessor
            id_successor ! Set_Predecessor(self)
            self ! Iterate_FTable
        }

        case Join_Chord => {
            if ( bootNode != null ) {
                println("Join_Chord: Find pred for "+node_id+" using "+getNodeID(bootNode))
                bootNode ! Finding_Predecessor(this.ftb(0).start,"booting:0",self,bootNode,bootNode_suc)
            } else {
                println("Join_Chord: Else")
                bootNode = self
                bootNode_suc = this.ftb(0).node
            }
        }
    }

    def lies_in(id:Integer,lb_val:Integer,hb_val:Integer,lbrac:Integer,rbrac:Integer): Boolean = {
        var lb:Integer = lb_val
        var hb:Integer = hb_val
        
        if((lbrac == 0) && (rbrac == 0)) {
            if(lb == hb){
                return false
            }
        }
        
        if(lbrac==0){
                lb=((lb+1)%pow(2,m)).toInt
        }

        if(rbrac==0){
            hb = (( ( (hb-1) % pow(2,m) ) + pow(2,m) ) % pow(2,m)).toInt
        }

        if(lb > hb){
            if( ( id >= lb && id <= pow(2,m)) || ( id >=0 && id <= hb ) ) {
                return true
            }
        } else if( id>= lb && id <= hb){
            return true
        } else {
            return false
        }
        return false
    }

    //Function that performs consistent hashing using SHA-1.
    def consistent_hash(nodeid:String) :Integer = {
        var flag:Integer = 0
        var ct=0
        var message_digest= MessageDigest.getInstance("SHA-1")
        message_digest.update(nodeid.getBytes())
        var hash_value=message_digest.digest()
        var hexstring= new StringBuffer()
        for ( i <- 0 to (hash_value.length-1)) {
            var hex = Integer.toHexString(0xff & hash_value(i))
            if(hex.length() == 1) hexstring.append('0')
            hexstring.append(hex)
        }
        
        var hex_string:String=hexstring.toString()
        var binary_string:String= new BigInteger(hex_string, 16).toString(2);
        binary_string=binary_string.substring(0,m-1)
        var hash_int :Integer= Integer.parseInt(binary_string,2)
        return hash_int    
    }
}

object ChordProtocol extends App{
    println("started")
    import Globs._
    
    var no_of_nodes=args(0).toInt
    val system= ActorSystem("ChordProtocol")
    var new_node:ActorRef = null
    var y:Integer = 0 //node name change
    for (i <- 1 to no_of_nodes) {
        new_node = system.actorOf(Props(new Node()), name = "node"+(i+y))
        new_node ! Join_Chord
        Thread.sleep(2000)
        //println("\n\n Boot Table")
        //system.actorSelection("/user/node1") ! Print_Node
        //println("\n\n")
        //Thread.sleep(5000)
    }
    Thread.sleep(5000)

    println("final print")
    for (i <- 1 to no_of_nodes) {

        system.actorSelection("/user/node"+(i+y)) ! Print_Node
        Thread.sleep(500)
    }
    system.shutdown
}