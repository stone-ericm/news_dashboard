"""Data export endpoints for News Dashboard."""

import io
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from fastapi import APIRouter, HTTPException, Query, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from src.cache.manager import get_cache_manager
from src.models.taxonomy import get_default_taxonomy
from src.models.source_registry import SourceRegistry
from src.api.historical import _generate_historical_data

router = APIRouter(prefix="/api/export", tags=["export"])


class ExportRequest(BaseModel):
    """Export request configuration."""
    format: str  # 'csv', 'json', 'excel'
    topic_ids: Optional[List[str]] = None
    start_date: Optional[str] = None  # YYYY-MM-DD
    end_date: Optional[str] = None    # YYYY-MM-DD
    include_metadata: bool = True
    include_processed: bool = True  # Include z-scores, anomalies, etc.
    data_sources: Optional[List[str]] = None


@router.get("/topics/{topic_id}/csv")
async def export_topic_csv(
    topic_id: str,
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    include_processed: bool = Query(True, description="Include processed metrics"),
    data_sources: Optional[List[str]] = Query(None, description="Data sources to include")
) -> StreamingResponse:
    """Export topic data as CSV."""
    try:
        # Validate dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start_dt >= end_dt:
            raise HTTPException(status_code=400, detail="Start date must be before end date")
        
        # Get topic info
        taxonomy = get_default_taxonomy()
        topic = taxonomy.get_topic(topic_id)
        if not topic:
            raise HTTPException(status_code=404, detail="Topic not found")
        
        # Generate data
        data_points = await _generate_historical_data(
            topic, start_dt, end_dt, data_sources, include_processed
        )
        
        # Convert to DataFrame
        df_data = []
        for dp in data_points:
            row = {
                'date': dp.date,
                'value': dp.value,
                'source': dp.source,
                'raw_value': dp.raw_value
            }
            if include_processed:
                row['z_score'] = dp.z_score
                row['is_anomaly'] = dp.is_anomaly
            df_data.append(row)
        
        df = pd.DataFrame(df_data)
        
        # Add metadata as header comments
        metadata_lines = [
            f"# News Dashboard Export",
            f"# Topic: {topic.name} ({topic_id})",
            f"# Date Range: {start_date} to {end_date}",
            f"# Generated: {datetime.utcnow().isoformat()}",
            f"# Data Sources: {', '.join(data_sources) if data_sources else 'All available'}",
            f"# Total Records: {len(df)}",
            f"#"
        ]
        
        # Create CSV content
        output = io.StringIO()
        
        # Write metadata
        for line in metadata_lines:
            output.write(line + '\n')
        
        # Write CSV data
        df.to_csv(output, index=False)
        output.seek(0)
        
        # Create filename
        filename = f"{topic_id}_{start_date}_{end_date}.csv"
        
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode('utf-8')),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error exporting data: {str(e)}")


@router.get("/topics/{topic_id}/json")
async def export_topic_json(
    topic_id: str,
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    include_processed: bool = Query(True, description="Include processed metrics"),
    data_sources: Optional[List[str]] = Query(None, description="Data sources to include"),
    pretty: bool = Query(True, description="Pretty print JSON")
) -> Response:
    """Export topic data as JSON."""
    try:
        # Validate dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start_dt >= end_dt:
            raise HTTPException(status_code=400, detail="Start date must be before end date")
        
        # Get topic info
        taxonomy = get_default_taxonomy()
        topic = taxonomy.get_topic(topic_id)
        if not topic:
            raise HTTPException(status_code=404, detail="Topic not found")
        
        # Generate data
        data_points = await _generate_historical_data(
            topic, start_dt, end_dt, data_sources, include_processed
        )
        
        # Calculate summary statistics
        values = [dp.value for dp in data_points]
        summary = {
            'total_points': len(data_points),
            'date_range': {
                'start': start_date,
                'end': end_date,
                'days': (end_dt - start_dt).days
            },
            'statistics': {
                'mean': float(np.mean(values)) if values else 0,
                'std': float(np.std(values)) if values else 0,
                'min': float(np.min(values)) if values else 0,
                'max': float(np.max(values)) if values else 0,
                'median': float(np.median(values)) if values else 0
            },
            'data_sources': list(set(dp.source for dp in data_points))
        }
        
        if include_processed:
            anomalies = [dp for dp in data_points if dp.is_anomaly]
            summary['anomalies'] = {
                'count': len(anomalies),
                'percentage': (len(anomalies) / len(data_points) * 100) if data_points else 0,
                'dates': [dp.date for dp in anomalies]
            }
        
        # Create export data
        export_data = {
            'metadata': {
                'topic': {
                    'id': topic_id,
                    'name': topic.name,
                    'description': topic.description,
                    'keywords': topic.keywords
                },
                'export': {
                    'generated_at': datetime.utcnow().isoformat(),
                    'format': 'json',
                    'include_processed': include_processed,
                    'requested_sources': data_sources
                }
            },
            'summary': summary,
            'data': [dp.dict() for dp in data_points]
        }
        
        # Convert to JSON
        if pretty:
            json_content = json.dumps(export_data, indent=2, default=str)
        else:
            json_content = json.dumps(export_data, default=str)
        
        # Create filename
        filename = f"{topic_id}_{start_date}_{end_date}.json"
        
        return Response(
            content=json_content,
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error exporting data: {str(e)}")


@router.get("/topics/{topic_id}/excel")
async def export_topic_excel(
    topic_id: str,
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    include_processed: bool = Query(True, description="Include processed metrics"),
    data_sources: Optional[List[str]] = Query(None, description="Data sources to include")
) -> StreamingResponse:
    """Export topic data as Excel file."""
    try:
        # Validate dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start_dt >= end_dt:
            raise HTTPException(status_code=400, detail="Start date must be before end date")
        
        # Get topic info
        taxonomy = get_default_taxonomy()
        topic = taxonomy.get_topic(topic_id)
        if not topic:
            raise HTTPException(status_code=404, detail="Topic not found")
        
        # Generate data
        data_points = await _generate_historical_data(
            topic, start_dt, end_dt, data_sources, include_processed
        )
        
        # Convert to DataFrame
        df_data = []
        for dp in data_points:
            row = {
                'Date': dp.date,
                'Value': dp.value,
                'Source': dp.source,
                'Raw Value': dp.raw_value
            }
            if include_processed:
                row['Z-Score'] = dp.z_score
                row['Is Anomaly'] = dp.is_anomaly
            df_data.append(row)
        
        df = pd.DataFrame(df_data)
        
        # Create Excel file in memory
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Main data sheet
            df.to_excel(writer, sheet_name='Data', index=False)
            
            # Summary sheet
            values = [dp.value for dp in data_points]
            summary_data = {
                'Metric': [
                    'Topic ID', 'Topic Name', 'Start Date', 'End Date', 'Total Records',
                    'Mean Value', 'Std Deviation', 'Min Value', 'Max Value', 'Median Value'
                ],
                'Value': [
                    topic_id, topic.name, start_date, end_date, len(data_points),
                    float(np.mean(values)) if values else 0,
                    float(np.std(values)) if values else 0,
                    float(np.min(values)) if values else 0,
                    float(np.max(values)) if values else 0,
                    float(np.median(values)) if values else 0
                ]
            }
            
            if include_processed:
                anomalies = [dp for dp in data_points if dp.is_anomaly]
                summary_data['Metric'].extend(['Anomalies Count', 'Anomaly Percentage'])
                summary_data['Value'].extend([
                    len(anomalies),
                    f"{(len(anomalies) / len(data_points) * 100):.1f}%" if data_points else "0%"
                ])
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Data sources sheet
            sources_data = []
            for source in set(dp.source for dp in data_points):
                source_points = [dp for dp in data_points if dp.source == source]
                source_values = [dp.value for dp in source_points]
                
                sources_data.append({
                    'Source': source,
                    'Records': len(source_points),
                    'Mean Value': float(np.mean(source_values)) if source_values else 0,
                    'Std Deviation': float(np.std(source_values)) if source_values else 0,
                    'Date Range': f"{min(dp.date for dp in source_points)} to {max(dp.date for dp in source_points)}"
                })
            
            sources_df = pd.DataFrame(sources_data)
            sources_df.to_excel(writer, sheet_name='Sources', index=False)
        
        output.seek(0)
        
        # Create filename
        filename = f"{topic_id}_{start_date}_{end_date}.xlsx"
        
        return StreamingResponse(
            io.BytesIO(output.getvalue()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error exporting Excel: {str(e)}")


@router.get("/bulk/topics")
async def export_multiple_topics(
    topic_ids: List[str] = Query(..., description="Topic IDs to export"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    format: str = Query("json", description="Export format (json, csv)"),
    include_processed: bool = Query(True, description="Include processed metrics")
) -> StreamingResponse:
    """Export data for multiple topics."""
    try:
        # Validate dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start_dt >= end_dt:
            raise HTTPException(status_code=400, detail="Start date must be before end date")
        
        if len(topic_ids) > 20:
            raise HTTPException(status_code=400, detail="Maximum 20 topics allowed for bulk export")
        
        # Get taxonomy
        taxonomy = get_default_taxonomy()
        
        # Collect data for all topics
        all_data = {}
        
        for topic_id in topic_ids:
            topic = taxonomy.get_topic(topic_id)
            if not topic:
                continue  # Skip invalid topics
            
            data_points = await _generate_historical_data(
                topic, start_dt, end_dt, None, include_processed
            )
            
            all_data[topic_id] = {
                'topic_info': {
                    'id': topic_id,
                    'name': topic.name,
                    'description': topic.description
                },
                'data_points': [dp.dict() for dp in data_points],
                'summary': {
                    'total_points': len(data_points),
                    'sources': list(set(dp.source for dp in data_points))
                }
            }
        
        if format.lower() == 'json':
            # JSON export
            export_data = {
                'metadata': {
                    'export_type': 'bulk_topics',
                    'date_range': {'start': start_date, 'end': end_date},
                    'topics_requested': topic_ids,
                    'topics_exported': list(all_data.keys()),
                    'generated_at': datetime.utcnow().isoformat(),
                    'include_processed': include_processed
                },
                'topics': all_data
            }
            
            json_content = json.dumps(export_data, indent=2, default=str)
            filename = f"bulk_export_{start_date}_{end_date}.json"
            
            return StreamingResponse(
                io.BytesIO(json_content.encode('utf-8')),
                media_type="application/json",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
        
        elif format.lower() == 'csv':
            # CSV export (combined data)
            output = io.StringIO()
            
            # Write metadata
            output.write(f"# News Dashboard Bulk Export\n")
            output.write(f"# Date Range: {start_date} to {end_date}\n")
            output.write(f"# Topics: {', '.join(topic_ids)}\n")
            output.write(f"# Generated: {datetime.utcnow().isoformat()}\n")
            output.write(f"#\n")
            
            # Combine all data into single DataFrame
            combined_data = []
            for topic_id, topic_data in all_data.items():
                for dp_dict in topic_data['data_points']:
                    row = {
                        'topic_id': topic_id,
                        'topic_name': topic_data['topic_info']['name'],
                        **dp_dict
                    }
                    combined_data.append(row)
            
            if combined_data:
                df = pd.DataFrame(combined_data)
                df.to_csv(output, index=False)
            
            output.seek(0)
            filename = f"bulk_export_{start_date}_{end_date}.csv"
            
            return StreamingResponse(
                io.BytesIO(output.getvalue().encode('utf-8')),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
        
        else:
            raise HTTPException(status_code=400, detail="Unsupported format. Use 'json' or 'csv'")
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in bulk export: {str(e)}")


@router.get("/sources/registry")
async def export_source_registry(format: str = Query("json", description="Export format (json, csv)")) -> Response:
    """Export the complete source registry."""
    try:
        registry = SourceRegistry()
        
        if format.lower() == 'json':
            # JSON export
            sources_data = []
            for source in registry.sources.values():
                sources_data.append({
                    'source_id': source.source_id,
                    'name': source.name,
                    'description': source.description,
                    'category': source.category,
                    'standard': source.standard.value,
                    'cadence_expected': source.cadence_expected.value,
                    'geographic_scope': source.geographic_scope.value,
                    'auth_method': source.auth_method.value,
                    'health_status': source.health_status.value,
                    'is_active': source.is_active,
                    'base_url': source.base_url,
                    'last_success_ts': source.last_success_ts.isoformat() if source.last_success_ts else None,
                    'created_at': source.created_at.isoformat(),
                    'updated_at': source.updated_at.isoformat()
                })
            
            export_data = {
                'metadata': {
                    'export_type': 'source_registry',
                    'total_sources': len(sources_data),
                    'generated_at': datetime.utcnow().isoformat()
                },
                'sources': sources_data
            }
            
            json_content = json.dumps(export_data, indent=2, default=str)
            filename = f"source_registry_{datetime.utcnow().strftime('%Y%m%d')}.json"
            
            return Response(
                content=json_content,
                media_type="application/json",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
        
        elif format.lower() == 'csv':
            # CSV export
            sources_data = []
            for source in registry.sources.values():
                sources_data.append({
                    'Source ID': source.source_id,
                    'Name': source.name,
                    'Description': source.description,
                    'Category': source.category,
                    'Data Standard': source.standard.value,
                    'Update Frequency': source.cadence_expected.value,
                    'Geographic Scope': source.geographic_scope.value,
                    'Auth Required': source.auth_method.value != 'none',
                    'Health Status': source.health_status.value,
                    'Is Active': source.is_active,
                    'Base URL': source.base_url,
                    'Last Success': source.last_success_ts.isoformat() if source.last_success_ts else '',
                    'Created': source.created_at.isoformat(),
                    'Updated': source.updated_at.isoformat()
                })
            
            df = pd.DataFrame(sources_data)
            
            output = io.StringIO()
            output.write(f"# News Dashboard Source Registry\n")
            output.write(f"# Generated: {datetime.utcnow().isoformat()}\n")
            output.write(f"# Total Sources: {len(sources_data)}\n")
            output.write(f"#\n")
            
            df.to_csv(output, index=False)
            output.seek(0)
            
            filename = f"source_registry_{datetime.utcnow().strftime('%Y%m%d')}.csv"
            
            return Response(
                content=output.getvalue(),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
        
        else:
            raise HTTPException(status_code=400, detail="Unsupported format. Use 'json' or 'csv'")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error exporting source registry: {str(e)}")


@router.get("/cache/data")
async def export_cache_data(format: str = Query("json", description="Export format (json, csv)")) -> Response:
    """Export cache statistics and metadata."""
    try:
        cache = get_cache_manager()
        stats = cache.get_stats()
        
        if format.lower() == 'json':
            # Add detailed cache entries
            cache_entries = []
            for key, entry in cache.index.items():
                cache_entries.append({
                    'key': entry.key,
                    'data_type': entry.data_type,
                    'created_at': entry.created_at.isoformat(),
                    'expires_at': entry.expires_at.isoformat() if entry.expires_at else None,
                    'size_bytes': entry.size_bytes,
                    'access_count': entry.access_count,
                    'last_accessed': entry.last_accessed.isoformat(),
                    'tags': entry.tags
                })
            
            export_data = {
                'metadata': {
                    'export_type': 'cache_data',
                    'generated_at': datetime.utcnow().isoformat()
                },
                'statistics': stats,
                'entries': cache_entries
            }
            
            json_content = json.dumps(export_data, indent=2, default=str)
            filename = f"cache_data_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.json"
            
            return Response(
                content=json_content,
                media_type="application/json",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
        
        else:
            raise HTTPException(status_code=400, detail="Only JSON format supported for cache data")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error exporting cache data: {str(e)}")


@router.get("/formats")
async def get_supported_formats() -> Dict:
    """Get list of supported export formats."""
    return {
        'formats': {
            'csv': {
                'description': 'Comma-separated values',
                'mime_type': 'text/csv',
                'supports_metadata': True,
                'best_for': 'Data analysis, spreadsheet import'
            },
            'json': {
                'description': 'JavaScript Object Notation',
                'mime_type': 'application/json',
                'supports_metadata': True,
                'best_for': 'API integration, web applications'
            },
            'excel': {
                'description': 'Microsoft Excel format',
                'mime_type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'supports_metadata': True,
                'best_for': 'Business reporting, multi-sheet analysis'
            }
        },
        'endpoints': {
            'single_topic': ['/export/topics/{topic_id}/csv', '/export/topics/{topic_id}/json', '/export/topics/{topic_id}/excel'],
            'bulk_topics': ['/export/bulk/topics'],
            'source_registry': ['/export/sources/registry'],
            'cache_data': ['/export/cache/data']
        }
    }
