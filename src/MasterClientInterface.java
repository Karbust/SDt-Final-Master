import java.rmi.Remote;

public interface MasterClientInterface extends Remote {
    void task(int fileCount) throws Exception;
    int checkTaskResult() throws Exception;
}
