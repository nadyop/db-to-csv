package id.nad.batch.config;

import org.joda.time.DateTime;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class BatchListener implements JobExecutionListener {

    private DateTime startTime, stopTime;

//    ambil lama waktu dalam milisecond
    private long getDuringTimeMs(DateTime start, DateTime stop){
        return stop.getMillis() - start.getMillis();
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        startTime = new DateTime();
        System.out.println("Start at " + startTime);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        stopTime = new DateTime();
        System.out.println("Stop at " + stopTime);
        System.out.println("Total time " + getDuringTimeMs(startTime, stopTime));

        if (jobExecution.getStatus() == BatchStatus.COMPLETED){
            System.out.println("Job completed!");
        }else{
            System.out.println("Job failed!");
            List<Throwable> throwableList = jobExecution.getAllFailureExceptions();
            for (Throwable th : throwableList){
                System.out.println("Throwable error " + th.getLocalizedMessage());
            }
        }
    }
}
