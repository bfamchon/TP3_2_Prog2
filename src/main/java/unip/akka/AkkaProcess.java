package unip.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import akka.actor.Props;
import scala.concurrent.duration.Duration;
import unip.mappers.Mapper;
import unip.master.Master;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static akka.actor.TypedActor.self;

/**
 * Created by baptiste on 08/06/17.
 * Hi
 */
public class AkkaProcess {

    private ActorSystem system;
    private List<ActorRef> mappers = new ArrayList<>();
    private List<String> reducersUrls = new ArrayList<>();
    private ActorRef master;

    public AkkaProcess() {
        this.system = ActorSystem.create("MySystem2");
    }

    public void run() {

        String fileUrl = "textes/zola-assommoir.txt";

        this.master = this.system.actorOf(Props.create(Master.class), "master");

        this.reducersUrls.add("akka.tcp://MySystem1@localhost:2553/user/red0");
        this.reducersUrls.add("akka.tcp://MySystem1@localhost:2553/user/red1");

        this.mappers.add(this.system.actorOf(Props.create(Mapper.class, "map0", this.reducersUrls), "map0"));
        this.mappers.add(this.system.actorOf(Props.create(Mapper.class, "map1", this.reducersUrls), "map1"));
        this.mappers.add(this.system.actorOf(Props.create(Mapper.class, "map2", this.reducersUrls), "map2"));

        this.master.tell(fileUrl,ActorRef.noSender());
    }
}
