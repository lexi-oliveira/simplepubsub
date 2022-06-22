package appl;

import core.Message;

import java.util.*;

public class SingleUser {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        new SingleUser();
    }

    public SingleUser() {
        Scanner reader = new Scanner(System.in);  // Reading from System.in

		System.out.print("Enter the Broker port number: ");
		int brokerPort = reader.nextInt();

		System.out.print("Enter the Broker address: ");
		String brokerAdd = reader.next();
		
		System.out.print("Enter the User address: ");
		String userAdd = reader.next();

        System.out.print("Enter the User name: ");
        String userName = reader.next();

        System.out.print("Enter the User port number: ");
        int userPort = reader.nextInt();

        System.out.print("Enter the number of acquires you want: ");
        int acquiresTotal = reader.nextInt();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        PubSubClient user = new PubSubClient(userAdd, userPort);

        user.subscribe(brokerAdd, brokerPort);

        startTP2(user, userName, brokerPort, brokerAdd, acquiresTotal);
    }

    private void startTP2(PubSubClient user, String userName, int brokerPort, String brokerAdd, int acquiresTotal) {
        String[] resources = {"var X"};

        int acquirePositionFinded = -1;

        Thread sendOneMsg = new ThreadWrapper(user, userName + ":" + "acquire", brokerAdd, brokerPort);

        sendOneMsg.start();

        try {
            sendOneMsg.join();
        } catch (Exception e) {
            e.printStackTrace();
        }

        //fazendo a obtencao dos notifies do broker

        /*
        while(true){
            List<Message> logUser = user.getLogMessages();
            treatLog(logUser, user, userName, brokerAdd, brokerPort, acquirePositionFinded);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

         */

        for(int i=0; i<acquiresTotal; i++){
            List<Message> logUser = user.getLogMessages();
            treatLog(logUser, user, userName, brokerAdd, brokerPort, acquirePositionFinded);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        sendPrimitive(user, userName, brokerAdd, brokerPort, "release");
        user.unsubscribe(brokerAdd, brokerPort);

        user.stopPubSubClient();


    }

    private void treatLog(List<Message> logUser, PubSubClient user, String userName, String brokerAdd, int brokerPort, int acquirePositionFinded) {
        //aqui existe toda a lógica do protocolo do TP2
        //se permanece neste método até que o acesso a VAR X ou VAR Y ou VAR Z ocorra

        int acquiresSended = 0;
        int releasesSended = 0;
        int position = 0;

        printUserLog(logUser);

        Iterator<Message> it = logUser.iterator();

        int myCurrentAcquire = findAcquireUserPosition(logUser, userName, acquirePositionFinded);

        acquiresSended = qtddeAcquiresAteoLogAtual(logUser, "", myCurrentAcquire);
        releasesSended = qtddeReleasesAteoLogAtual(logUser, "", myCurrentAcquire);

        while (it.hasNext()){
            Message aux = it.next();

            position++;


            if(isMyTurn(aux, userName, myCurrentAcquire, releasesSended, acquiresSended, position)){
                Random seed = new Random();
                int sec = seed.nextInt(3);
                System.out.println("\n\nEm " + (aux.getLogId()-2) + " " + userName + " entrou na regiao critica por " + sec + " segundos");

                try {
                    Thread.sleep(1000 * sec);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                System.out.println(userName + " saiu da regiao critica\n\n");

                sendPrimitive(user, userName, brokerAdd, brokerPort, "release");

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                sendPrimitive(user, userName, brokerAdd, brokerPort, "acquire");

            }
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private boolean isMyTurn(Message message, String userName, int acquirePositionFinded , int releasesSended, int acquiresSended, int position) {
        String messageContent[];
        messageContent = message.getContent().split(":");

        return (Objects.equals(messageContent[0], userName)) && (releasesSended == acquiresSended  - 1) && (isAcquire(message)) && (acquirePositionFinded == message.getLogId()-2);
    }

    private void printUserLog(List<Message> logUser) {
        Iterator<Message> it = logUser.iterator();
        System.out.print("Log User itens: \n");
        while (it.hasNext()) {
            Message aux = it.next();
            System.out.print(aux.getLogId() - 2 + " - " + aux.getContent() + " | ");
        }
        System.out.println();
    }

    private boolean isAcquire(Message message){
        String messageContent[];
        messageContent = message.getContent().split(":");

        return (Objects.equals(messageContent[1], "acquire"));
    }

    private boolean isRelease(Message message){
        String messageContent[];
        messageContent = message.getContent().split(":");

        return (Objects.equals(messageContent[1], "release"));
    }

    private void sendPrimitive(PubSubClient user, String userName, String brokerAdd, int brokerPort, String primitive){
        Thread sendOneMsg = new ThreadWrapper(user, userName + ":" + primitive, brokerAdd, brokerPort);

        sendOneMsg.start();

        try {
            sendOneMsg.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private int findAcquireUserPosition(List<Message> logUser, String userName, int acquirePositionFinded){
        int lastAcquire = 0;
        for (Message aux : logUser) {
            if (Objects.equals(getMessageUser(aux), userName) &&
                    Objects.equals(getMessagePrimitive(aux), "acquire")
            ) {
                lastAcquire = aux.getLogId() - 2;
            }
        }

        return lastAcquire;
    }

    private int qtddeAcquiresAteoLogAtual(List<Message> logUser, String primitive, int offset){
        int qtt = 0;
        Iterator<Message> it = logUser.iterator();
        boolean encontrouAtual = false;

        while (it.hasNext() && !encontrouAtual){
            Message aux = it.next();
            if(isAcquire(aux)){
                qtt++;
            }
            if(aux.getLogId()-2 == offset){
                encontrouAtual = true;
            }
        }

        return qtt;

    }

    private int qtddeReleasesAteoLogAtual(List<Message> logUser, String primitive, int offset){
        int qtt = 0;
        Iterator<Message> it = logUser.iterator();
        boolean encontrouAtual = false;

        while (it.hasNext()){
            Message aux = it.next();
            if(isRelease(aux)){
                qtt++;
            }
            if(aux.getLogId()-2 == offset){
                encontrouAtual = true;
            }
        }

        return qtt;

    }

    private String getMessageContent(Message message, int positionContent) {
        String messageContent[];
        messageContent = message.getContent().split(":");

        return messageContent[positionContent];
    }

    private String getMessageUser(Message message){
        return getMessageContent(message,0);
    }

    private String getMessagePrimitive(Message message){
        return getMessageContent(message,1);
    }


    class ThreadWrapper extends Thread {
        PubSubClient c;
        String msg;
        String host;
        int port;

        public ThreadWrapper(PubSubClient c, String msg, String host, int port) {
            this.c = c;
            this.msg = msg;
            this.host = host;
            this.port = port;
        }

        public void run() {
            c.publish(msg, host, port);
        }
    }

}
