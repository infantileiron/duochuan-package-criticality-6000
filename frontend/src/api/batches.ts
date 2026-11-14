import {request} from './client';import type {ImportBatch} from '../types/domain';
export const list=()=>request<ImportBatch[]>('/batches');
export const create=(data:unknown)=>request<ImportBatch>('/batches',data);
