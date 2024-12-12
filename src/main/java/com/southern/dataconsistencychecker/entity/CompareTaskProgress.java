package com.southern.dataconsistencychecker.entity;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class CompareTaskProgress implements Serializable {
    private Long taskId;
    private Integer partitionId;
    private String status; // e.g., 'PENDING', 'IN_PROGRESS', 'COMPLETED'
    private String lastProcessedKey;

    // toString, equals, hashCode 方法（可选）

    @Override
    public String toString() {
        return "CompareTaskProgress{" +
                "taskId=" + taskId +
                ", partitionId=" + partitionId +
                ", status='" + status + '\'' +
                ", lastProcessedKey='" + lastProcessedKey + '\'' +
                '}';
    }
}
