import torch
from sentence_transformers import SentenceTransformer
import numpy as np
from io import BytesIO

class EmbeddingGenerator:
    """
    Handles the text chunking and embedding generation using a sentence-transformer model.
    """
    def __init__(self, config):
        model_config = config['models']['embedding']
        self.model_name = model_config['model_name']
        self.chunk_size = model_config['chunk_size']
        self.chunk_overlap = model_config['chunk_overlap']
        
        # Auto-detect and use GPU if available
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"EmbeddingGenerator: Using device '{self.device}'")
        
        # Load the model onto the specified device
        self.model = SentenceTransformer(self.model_name, device=self.device)
        
        # The BGE model requires a specific instruction for retrieval tasks
        self.instruction = "Represent this sentence for searching relevant passages: "

    def _chunk_text(self, text):
        """Splits text into overlapping chunks."""
        chunks = []
        start = 0
        while start < len(text):
            end = start + self.chunk_size
            chunks.append(text[start:end])
            start += self.chunk_size - self.chunk_overlap
        return chunks

    def generate_embedding_for_text(self, text):
        """
        Generates a single embedding vector for a given block of text.
        It chunks the text, gets embeddings for each chunk, and averages them.
        """
        if not text or not text.strip():
            print("Warning: Received empty text for embedding. Returning None.")
            return None

        # 1. Chunk the text
        chunks = self._chunk_text(text)
        
        # 2. Prepend the instruction required by the BGE model
        chunks_with_instruction = [self.instruction + chunk for chunk in chunks]
        
        # 3. Generate embeddings for all chunks in a batch
        chunk_embeddings = self.model.encode(
            chunks_with_instruction,
            normalize_embeddings=True, # Important for similarity search
            show_progress_bar=False # Can be set to True for debugging single large files
        )
        
        # 4. Average the embeddings to get a single representative vector
        # This is a common and effective strategy for document-level embeddings from chunks
        document_embedding = np.mean(chunk_embeddings, axis=0)
        
        return document_embedding

    def save_embedding_to_bytes(self, embedding_vector):
        """Saves a numpy array to an in-memory bytes buffer."""
        bytes_io = BytesIO()
        np.save(bytes_io, embedding_vector)
        bytes_io.seek(0) # Rewind the buffer to the beginning
        return bytes_io

