package com.bmax.lograg.repository;

import com.bmax.lograg.model.AppLog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface AppLogRepository extends ElasticsearchRepository<AppLog, String> {

    Page<AppLog> findByLevel(String level, Pageable pageable);

    Page<AppLog> findByService(String service, Pageable pageable);

    Page<AppLog> findAllByOrderByTimestampDesc(Pageable pageable);

    List<AppLog> findByLevelIn(List<String> levels);

    long countByLevel(String level);
}
